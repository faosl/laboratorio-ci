using System.Diagnostics;
using Npgsql;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class PostgresInboxProcessingTests(DatabaseFixture database) : IClassFixture<DatabaseFixture>, IAsyncLifetime
{
    private static readonly CancellationToken Token = CancellationToken.None;
    private static readonly TimeSpan Duration = TimeSpan.FromMinutes(1);
    private PostgresInboxProcessingStore Store => new(database.Source);

    // Each test has its own queue contents; the class fixture has an isolated real database.
    public async Task InitializeAsync() => await Sql("TRUNCATE public.ordering_inbox");
    public Task DisposeAsync() => Task.CompletedTask;

    private async Task Sql(string sql, params object[] values)
    {
        await using var command = database.Source.CreateCommand(sql);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<Guid> Register()
    {
        var id = Guid.NewGuid().ToString();
        var result = await new PostgresInboxStore(database.Source).RegisterAsync(
            new(new("partner", "scope", id), new OrderNotification(id, "order"), 1, [0, 255, 10]), Token);
        return result.InboxId!.Value;
    }

    private async Task<AcquiredInboxNotification> Acquire(string owner = "worker")
    {
        var result = await Store.TryAcquireAsync(owner, Duration, Token);
        Assert.Equal(InboxAcquisitionStatus.Acquired, result.Status);
        return Assert.IsType<AcquiredInboxNotification>(result.Work);
    }

    private Task Expire(Guid id) => Sql("UPDATE public.ordering_inbox SET lease_expires_at_utc = clock_timestamp() - interval '1 second' WHERE inbox_id = $1", id);

    [Fact]
    public async Task ConcurrentIndependentConnections_ConfirmOnlyOneLeaseAndReleaseLocks()
    {
        var id = await Register();
        var results = await Task.WhenAll(Enumerable.Range(0, 24).Select(async i =>
        {
            await using var source = NpgsqlDataSource.Create(database.ConnectionString);
            return await new PostgresInboxProcessingStore(source).TryAcquireAsync("worker-" + i, Duration, Token);
        }));
        var acquired = Assert.Single(results, x => x.Status == InboxAcquisitionStatus.Acquired).Work!;
        Assert.Equal(id, acquired.Lease.InboxId);
        Assert.Equal(1, acquired.Lease.Generation);
        Assert.Equal(new byte[] { 0, 255, 10 }, acquired.Content.OriginalPayload);
        Assert.Equal(23, results.Count(x => x.Status == InboxAcquisitionStatus.NoEligibleWork && x.Work is null));
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand("SELECT inbox_id FROM public.ordering_inbox WHERE inbox_id = $1 FOR UPDATE NOWAIT", connection, transaction);
        command.Parameters.AddWithValue(id);
        Assert.Equal(id, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task SkipLocked_BypassesOldestLockedCandidate_AndDoesNotClaimEmptyInbox()
    {
        var first = await Register();
        await Sql("UPDATE public.ordering_inbox SET received_at_utc = clock_timestamp() - interval '1 day' WHERE inbox_id = $1", first);
        var second = await Register();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand("SELECT inbox_id FROM public.ordering_inbox WHERE inbox_id = $1 FOR UPDATE", connection, transaction);
        command.Parameters.AddWithValue(first);
        await command.ExecuteScalarAsync();
        Assert.Equal(second, (await Acquire()).Lease.InboxId);
        Assert.Equal(InboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status);
        await transaction.RollbackAsync();
        Assert.Equal(first, (await Acquire()).Lease.InboxId);
    }

    [Fact]
    public async Task ExpirationRecovery_FencesEveryOldOperation_EvenWithSameOwner()
    {
        await Register();
        var old = await Acquire();
        await Expire(old.Lease.InboxId);
        var current = await Acquire();
        Assert.Equal(old.Lease.Generation + 1, current.Lease.Generation);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.RenewAsync(old.Lease, Duration, Token)).Status);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.CompleteAsync(old.Lease, Token)).Status);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.ScheduleRetryAsync(old.Lease, TimeSpan.Zero, Token)).Status);
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.CompleteAsync(current.Lease, Token)).Status);
    }

    [Fact]
    public async Task ExpiredLeaseWithoutSuccessor_CannotMutate()
    {
        await Register();
        var work = await Acquire();
        await Expire(work.Lease.InboxId);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.RenewAsync(work.Lease, Duration, Token)).Status);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.CompleteAsync(work.Lease, Token)).Status);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.ScheduleRetryAsync(work.Lease, TimeSpan.Zero, Token)).Status);
    }

    [Fact]
    public async Task Renewal_ExtendsWithoutReducingExpirationOrChangingGeneration()
    {
        await Register();
        var work = await Acquire();
        var shorter = await Store.RenewAsync(work.Lease, TimeSpan.FromSeconds(1), Token);
        Assert.Equal(InboxMutationStatus.Succeeded, shorter.Status);
        Assert.Equal(work.ExpiresAtUtc, shorter.ExpiresAtUtc);
        var longer = await Store.RenewAsync(work.Lease, TimeSpan.FromMinutes(2), Token);
        Assert.True(longer.ExpiresAtUtc > work.ExpiresAtUtc);
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.CompleteAsync(work.Lease, Token)).Status);
        Assert.Equal(InboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status);
        await using var command = database.Source.CreateCommand("SELECT lease_generation = $2 AND lease_owner IS NULL AND lease_expires_at_utc IS NULL AND state = 'Completed' FROM public.ordering_inbox WHERE inbox_id = $1");
        command.Parameters.AddWithValue(work.Lease.InboxId);
        command.Parameters.AddWithValue(work.Lease.Generation);
        Assert.Equal(true, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Retry_IsReleasedAndEligibleOnlyWhenDue()
    {
        await Register();
        var work = await Acquire();
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.ScheduleRetryAsync(work.Lease, TimeSpan.FromHours(1), Token)).Status);
        Assert.Equal(InboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status);
        await using (var check = database.Source.CreateCommand("SELECT lease_owner IS NULL AND lease_expires_at_utc IS NULL AND lease_generation = $2 AND next_attempt_at_utc > clock_timestamp() FROM public.ordering_inbox WHERE inbox_id = $1"))
        {
            check.Parameters.AddWithValue(work.Lease.InboxId);
            check.Parameters.AddWithValue(work.Lease.Generation);
            Assert.Equal(true, await check.ExecuteScalarAsync());
        }
        await Sql("UPDATE public.ordering_inbox SET next_attempt_at_utc = clock_timestamp() - interval '1 second' WHERE inbox_id = $1", work.Lease.InboxId);
        var retried = await Acquire("other");
        Assert.Equal(work.Lease.Generation + 1, retried.Lease.Generation);
    }

    [Fact]
    public async Task RenewalVersusRecovery_SerializesAtLeaseBoundary()
    {
        await Register();
        var work = await Acquire();
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var renewing = new PostgresInboxProcessingStore(database.Source, new(AfterLock: async (_, _, ct) =>
        { reached.SetResult(); await release.Task.WaitAsync(TimeSpan.FromSeconds(5), ct); }));
        var pending = renewing.RenewAsync(work.Lease, Duration, Token);
        await reached.Task.WaitAsync(TimeSpan.FromSeconds(5));
        try { Assert.Equal(InboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status); }
        finally { release.TrySetResult(); }
        Assert.Equal(InboxMutationStatus.Succeeded, (await pending).Status);
        await Expire(work.Lease.InboxId);
        var renewal = Store.RenewAsync(work.Lease, Duration, Token);
        var recovery = Store.TryAcquireAsync("other", Duration, Token);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await renewal).Status);
        var recovered = await recovery;
        // A rejected renewal may briefly hold the row lock; SKIP LOCKED can legitimately see no work.
        if (recovered.Status == InboxAcquisitionStatus.NoEligibleWork)
            recovered = await Store.TryAcquireAsync("other", Duration, Token);
        Assert.Equal(InboxAcquisitionStatus.Acquired, recovered.Status);
        Assert.True(recovered.Work!.Lease.Generation > work.Lease.Generation);
    }

    private async Task WaitForLock(int processId)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        while (true)
        {
            await using var command = database.Source.CreateCommand("SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = $1 AND wait_event_type = 'Lock')");
            command.Parameters.AddWithValue(processId);
            if ((bool)(await command.ExecuteScalarAsync(timeout.Token))!) return;
            await Task.Delay(10, timeout.Token);
        }
    }

    [Theory]
    [InlineData("renew")]
    [InlineData("complete")]
    [InlineData("retry")]
    public async Task Mutation_ValidatesFreshTimeAfterLockWait(string operation)
    {
        await Register();
        var work = await Acquire();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var locked = new NpgsqlCommand("SELECT inbox_id FROM public.ordering_inbox WHERE inbox_id = $1 FOR UPDATE", connection, transaction);
        locked.Parameters.AddWithValue(work.Lease.InboxId);
        await locked.ExecuteScalarAsync();
        var started = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new PostgresInboxProcessingStore(database.Source, new(BeforeLock: async (c, tx, ct) =>
        {
            await using var timeout = new NpgsqlCommand("SET LOCAL lock_timeout = '8s'; SET LOCAL statement_timeout = '9s'", c, tx);
            await timeout.ExecuteNonQueryAsync(ct);
            started.SetResult(c.ProcessID);
        }));
        var pending = Mutate(store, work.Lease, operation, Token);
        await WaitForLock(await started.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        // Set expiry inside the observed wait, strictly after the waiting transaction started.
        // Using that transaction's now() instead of a fresh post-lock clock would incorrectly pass.
        await using var expire = new NpgsqlCommand("UPDATE public.ordering_inbox SET lease_expires_at_utc = clock_timestamp() WHERE inbox_id = $1", connection, transaction);
        expire.Parameters.AddWithValue(work.Lease.InboxId);
        await expire.ExecuteNonQueryAsync();
        await transaction.CommitAsync();
        Assert.Equal(InboxMutationStatus.LeaseLost, (await pending).Status);
    }

    private static Task<InboxMutationResult> Mutate(PostgresInboxProcessingStore store, InboxLease lease, string operation, CancellationToken ct) => operation switch
    {
        "renew" => store.RenewAsync(lease, Duration, ct),
        "complete" => store.CompleteAsync(lease, ct),
        _ => store.ScheduleRetryAsync(lease, TimeSpan.FromMinutes(1), ct)
    };

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Acquisition_LostCommitConfirmation_ReturnsNoUsableWork(bool commit)
    {
        await Register();
        var store = new PostgresInboxProcessingStore(database.Source, new(Commit: async (_, tx, ct) =>
        {
            if (commit) await tx.CommitAsync(ct); else await tx.RollbackAsync(ct);
            throw new IOException("Synthetic lost confirmation");
        }));
        var result = await store.TryAcquireAsync("uncertain", Duration, Token);
        Assert.Equal(InboxAcquisitionStatus.OutcomeUnknown, result.Status);
        Assert.Null(result.Work);
        Assert.Equal(commit ? InboxAcquisitionStatus.NoEligibleWork : InboxAcquisitionStatus.Acquired,
            (await Store.TryAcquireAsync("other", Duration, Token)).Status);
    }

    [Theory]
    [InlineData("renew", true)]
    [InlineData("renew", false)]
    [InlineData("complete", true)]
    [InlineData("complete", false)]
    [InlineData("retry", true)]
    [InlineData("retry", false)]
    public async Task Mutations_LostCommitConfirmation_DistinguishUnknownFromLostLease(string operation, bool commit)
    {
        await Register();
        var work = await Acquire();
        var before = await ProcessingSnapshot(work.Lease.InboxId);
        var store = new PostgresInboxProcessingStore(database.Source, new(Commit: async (_, tx, ct) =>
        {
            if (commit) await tx.CommitAsync(ct); else await tx.RollbackAsync(ct);
            throw new IOException("Synthetic lost confirmation");
        }));
        var result = await Mutate(store, work.Lease, operation, Token);
        Assert.Equal(InboxMutationStatus.OutcomeUnknown, result.Status);
        Assert.Null(result.ExpiresAtUtc);
        var after = await ProcessingSnapshot(work.Lease.InboxId);
        if (commit) Assert.NotEqual(before, after);
        else Assert.Equal(before, after);
        var expected = commit && operation != "renew" ? InboxMutationStatus.LeaseLost : InboxMutationStatus.Succeeded;
        Assert.Equal(expected, (await Store.CompleteAsync(work.Lease, Token)).Status);
    }

    private async Task<string> ProcessingSnapshot(Guid id)
    {
        await using var command = database.Source.CreateCommand("SELECT row_to_json(i)::text FROM public.ordering_inbox i WHERE inbox_id = $1");
        command.Parameters.AddWithValue(id);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    [Theory]
    [InlineData("renew")]
    [InlineData("complete")]
    [InlineData("retry")]
    public async Task MutationFailureBeforeCommit_RollsBackAndRetainsLease(string operation)
    {
        await Register();
        var work = await Acquire();
        var before = await ProcessingSnapshot(work.Lease.InboxId);
        var store = new PostgresInboxProcessingStore(database.Source, new(BeforeCommit: (_, _, _) => throw new IOException("Before commit")));
        await Assert.ThrowsAsync<IOException>(() => Mutate(store, work.Lease, operation, Token));
        Assert.Equal(before, await ProcessingSnapshot(work.Lease.InboxId));
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.CompleteAsync(work.Lease, Token)).Status);
    }

    [Theory]
    [InlineData("renew", true)]
    [InlineData("renew", false)]
    [InlineData("complete", true)]
    [InlineData("complete", false)]
    [InlineData("retry", true)]
    [InlineData("retry", false)]
    public async Task MutationCancellationOrTimeoutDuringCommit_IsUnknown(string operation, bool timeout)
    {
        await Register();
        var work = await Acquire();
        var store = new PostgresInboxProcessingStore(database.Source, new(Commit: async (_, tx, ct) =>
        {
            await tx.CommitAsync(ct);
            if (timeout) throw new TimeoutException("Lost response");
            throw new OperationCanceledException("Lost response");
        }));
        var result = await Mutate(store, work.Lease, operation, Token);
        Assert.Equal(InboxMutationStatus.OutcomeUnknown, result.Status);
        Assert.Null(result.ExpiresAtUtc);
    }

    [Fact]
    public async Task AcquisitionIndex_SupportsActualOrderedDueRangeQuery()
    {
        await Register();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        // Small fixtures normally favor a sequential scan. Disabling it verifies applicability,
        // not a production planner choice or a performance benchmark.
        await using (var settings = new NpgsqlCommand("SET LOCAL enable_seqscan = off", connection, transaction))
            await settings.ExecuteNonQueryAsync();
        await using var command = new NpgsqlCommand("EXPLAIN " + PostgresInboxProcessingStore.AcquisitionSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync();
        var lines = new List<string>();
        while (await reader.ReadAsync()) lines.Add(reader.GetString(0));
        var plan = string.Join('\n', lines);
        Assert.Contains("ordering_inbox_processing_due", plan);
        Assert.Contains("Index Cond", plan);
        Assert.DoesNotContain("Sort", plan);
    }

    [Fact]
    public async Task PreCommitFailure_RollsBackAcquisition()
    {
        await Register();
        var store = new PostgresInboxProcessingStore(database.Source, new(BeforeCommit: (_, _, _) => throw new IOException("Before commit")));
        await Assert.ThrowsAsync<IOException>(() => store.TryAcquireAsync("failed", Duration, Token));
        Assert.Equal(1, (await Acquire()).Lease.Generation);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CancellationOrTimeoutDuringCommit_IsUnknown(bool timeout)
    {
        await Register();
        var store = new PostgresInboxProcessingStore(database.Source, new(Commit: async (_, tx, ct) =>
        {
            await tx.CommitAsync(ct);
            if (timeout) throw new TimeoutException("Lost commit response");
            throw new OperationCanceledException("Lost commit response");
        }));
        Assert.Equal(InboxAcquisitionStatus.OutcomeUnknown, (await store.TryAcquireAsync("uncertain", Duration, Token)).Status);
        Assert.Equal(InboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status);
    }

    [Fact]
    public async Task CancellationBeforeCommit_RollsBackWithoutGrantingWork()
    {
        await Register();
        using var canceled = new CancellationTokenSource();
        var store = new PostgresInboxProcessingStore(database.Source, new(BeforeCommit: (_, _, ct) =>
        { canceled.Cancel(); ct.ThrowIfCancellationRequested(); return Task.CompletedTask; }));
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.TryAcquireAsync("canceled", Duration, canceled.Token));
        Assert.Equal(1, (await Acquire()).Lease.Generation);
    }

    [Fact]
    public async Task LockTimeout_IsBoundedAndDoesNotMutate()
    {
        await Register();
        var work = await Acquire();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand("SELECT inbox_id FROM public.ordering_inbox WHERE inbox_id = $1 FOR UPDATE", connection, tx);
        command.Parameters.AddWithValue(work.Lease.InboxId);
        await command.ExecuteScalarAsync();
        var error = await Record.ExceptionAsync(() => Store.CompleteAsync(work.Lease, Token));
        Assert.True(error is InboxStorageUnavailableException || error is PostgresException { SqlState: PostgresErrorCodes.LockNotAvailable });
        await tx.RollbackAsync();
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.CompleteAsync(work.Lease, Token)).Status);
    }

    [Fact]
    public async Task GenerationOverflow_FailsWithoutReusingToken()
    {
        var id = await Register();
        await Sql("UPDATE public.ordering_inbox SET lease_generation = $2 WHERE inbox_id = $1", id, long.MaxValue);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Store.TryAcquireAsync("worker", Duration, Token));
        await using var command = database.Source.CreateCommand("SELECT lease_generation FROM public.ordering_inbox WHERE inbox_id = $1");
        command.Parameters.AddWithValue(id);
        Assert.Equal(long.MaxValue, await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task IncorrectOwnerGenerationOrMissingRecord_IsLeaseLost()
    {
        await Register();
        var work = await Acquire();
        foreach (var lease in new[]
        {
            new InboxLease(work.Lease.InboxId, "wrong-owner", work.Lease.Generation),
            new InboxLease(work.Lease.InboxId, work.Lease.Owner, work.Lease.Generation + 1),
            new InboxLease(Guid.NewGuid(), work.Lease.Owner, work.Lease.Generation)
        })
        {
            Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.RenewAsync(lease, Duration, Token)).Status);
            Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.CompleteAsync(lease, Token)).Status);
            Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.ScheduleRetryAsync(lease, TimeSpan.Zero, Token)).Status);
        }
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.CompleteAsync(work.Lease, Token)).Status);
    }

    [Fact]
    public async Task ProvisioningIsRepeatable_AndStateConstraintsRejectIncoherentRows()
    {
        var id = await Register();
        await InboxSchema.ApplyAsync(database.Source);
        await InboxSchema.ApplyAsync(database.Source);
        foreach (var assignment in new[] { "state = 'Expired'", "state = 'Processing'", "state = 'RetryScheduled'", "lease_owner = 'orphan'", "lease_generation = -1" })
        {
            var error = await Assert.ThrowsAsync<PostgresException>(() => Sql($"UPDATE public.ordering_inbox SET {assignment} WHERE inbox_id = $1", id));
            Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
        }
        Assert.Equal(id, (await Acquire()).Lease.InboxId);
    }

    [Fact]
    public async Task ProcessRestart_RecoversPersistedLeaseWithGreaterGeneration()
    {
        var id = await Register();
        var first = await RunProbe("execution-one");
        Assert.Equal($"{id}|1", first);
        await Expire(id);
        var second = await RunProbe("execution-two");
        Assert.Equal($"{id}|2", second);
        Assert.Equal(InboxMutationStatus.LeaseLost, (await Store.CompleteAsync(new(id, "execution-one", 1), Token)).Status);
    }

    private async Task<string> RunProbe(string owner)
    {
        var root = new DirectoryInfo(AppContext.BaseDirectory);
        while (root is not null && !Directory.Exists(Path.Combine(root.FullName, "Ordering.Inbox.TestHost"))) root = root.Parent;
        Assert.NotNull(root);
        var configuration = new DirectoryInfo(AppContext.BaseDirectory).Parent!.Name;
        var dll = Path.Combine(root.FullName, "Ordering.Inbox.TestHost", "bin", configuration, "net8.0", "Ordering.Inbox.TestHost.dll");
        var start = new ProcessStartInfo("dotnet") { RedirectStandardOutput = true, RedirectStandardError = true, UseShellExecute = false };
        start.ArgumentList.Add(dll);
        start.ArgumentList.Add("acquire");
        start.ArgumentList.Add(owner);
        start.Environment["INBOX_TEST_CONNECTION"] = database.ConnectionString;
        using var process = Process.Start(start)!;
        var output = process.StandardOutput.ReadToEndAsync();
        var error = process.StandardError.ReadToEndAsync();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(20));
        try { await process.WaitForExitAsync(timeout.Token); }
        catch { process.Kill(entireProcessTree: true); throw; }
        Assert.True(process.ExitCode == 0, await error);
        return (await output).Trim();
    }

    [Fact]
    public async Task VersionOneUpgrade_PreservesExistingDataAndDeduplication()
    {
        var name = "inbox_upgrade_" + Guid.NewGuid().ToString("N");
        await Sql($"CREATE DATABASE {name}");
        var connectionString = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Database = name }.ConnectionString;
        var input = new InboxNotification(new("partner", "scope", "existing"), new OrderNotification("existing", "order"), 1, [0, 255, 10]);
        try
        {
            await using var source = NpgsqlDataSource.Create(connectionString);
            using var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream("Ordering.Infrastructure.PostgreSql.Migrations.001_inbox.sql")!;
            using var reader = new StreamReader(stream);
            await using (var setup = source.CreateCommand(await reader.ReadToEndAsync() + """
                CREATE TABLE public.ordering_inbox_schema_version (version integer PRIMARY KEY,
                    applied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp());
                INSERT INTO public.ordering_inbox_schema_version(version) VALUES (1);
                """)) await setup.ExecuteNonQueryAsync();
            var registration = new PostgresInboxStore(source);
            var inserted = await registration.RegisterAsync(input, Token);
            var before = (await registration.GetByKeyAsync(input.Key, Token))!;
            await InboxSchema.ApplyAsync(source);
            await InboxSchema.ApplyAsync(source);
            var after = (await registration.GetByKeyAsync(input.Key, Token))!;
            Assert.Equal(before.InboxId, after.InboxId);
            Assert.Equal(before.ReceivedAtUtc, after.ReceivedAtUtc);
            Assert.Equal(before.Content.OriginalPayload, after.Content.OriginalPayload);
            Assert.Equal("Pending", after.State);
            await using (var check = source.CreateCommand("SELECT lease_generation = 0 AND lease_owner IS NULL AND lease_expires_at_utc IS NULL AND next_attempt_at_utc IS NULL FROM public.ordering_inbox"))
                Assert.Equal(true, await check.ExecuteScalarAsync());
            Assert.Equal(InboxRegistrationStatus.AlreadyExists, (await registration.RegisterAsync(input, Token)).Status);
            var acquired = await new PostgresInboxProcessingStore(source).TryAcquireAsync("new", Duration, Token);
            Assert.Equal(inserted.InboxId, acquired.Work!.Lease.InboxId);
            Assert.Equal(1, acquired.Work.Lease.Generation);
        }
        finally { await Sql($"DROP DATABASE {name} WITH (FORCE)"); }
    }

    [Fact]
    public async Task AcquisitionRevalidatesDatabaseTimeAfterSelection()
    {
        await Register();
        var work = await Acquire();
        await Expire(work.Lease.InboxId);
        // Move the due time under our own lock to deterministically exercise failed revalidation.
        // This models a civil-clock boundary change without changing the host/server clock.
        var store = new PostgresInboxProcessingStore(database.Source, new(AfterLock: async (c, tx, ct) =>
        {
            await using var command = new NpgsqlCommand("UPDATE public.ordering_inbox SET lease_expires_at_utc = clock_timestamp() + interval '1 hour'", c, tx);
            await command.ExecuteNonQueryAsync(ct);
        }));
        Assert.Equal(InboxAcquisitionStatus.NoEligibleWork, (await store.TryAcquireAsync("other", Duration, Token)).Status);
        // No commit occurred, including the synthetic change.
        Assert.Equal(work.Lease.Generation + 1, (await Acquire()).Lease.Generation);
    }

    [Fact]
    public async Task CancellationDuringLockWait_DoesNotMutate()
    {
        await Register();
        var work = await Acquire();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var tx = await connection.BeginTransactionAsync();
        await using var command = new NpgsqlCommand("SELECT inbox_id FROM public.ordering_inbox FOR UPDATE", connection, tx);
        await command.ExecuteScalarAsync();
        var started = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new PostgresInboxProcessingStore(database.Source, new(BeforeLock: async (c, tx, ct) =>
        {
            await using var timeout = new NpgsqlCommand("SET LOCAL lock_timeout = '8s'; SET LOCAL statement_timeout = '9s'", c, tx);
            await timeout.ExecuteNonQueryAsync(ct);
            started.SetResult(c.ProcessID);
        }));
        using var canceled = new CancellationTokenSource();
        var pending = store.CompleteAsync(work.Lease, canceled.Token);
        await WaitForLock(await started.Task.WaitAsync(TimeSpan.FromSeconds(5)));
        canceled.Cancel();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        await tx.RollbackAsync();
        Assert.Equal(InboxMutationStatus.Succeeded, (await Store.CompleteAsync(work.Lease, Token)).Status);
    }
}
