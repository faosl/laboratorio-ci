using Npgsql;
using Ordering.Application.Outbox;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class PostgresOutboxProcessingTests(DatabaseFixture database) : IClassFixture<DatabaseFixture>, IAsyncLifetime
{
    private static readonly CancellationToken Token = CancellationToken.None;
    private static readonly TimeSpan Duration = TimeSpan.FromMinutes(1);
    private PostgresOutboxProcessingStore Store => new(database.Source);

    public async Task InitializeAsync() => await Sql("TRUNCATE public.ordering_outbox_messages, public.ordering_lab_orders");
    public Task DisposeAsync() => Task.CompletedTask;

    private async Task Sql(string sql, params object[] values)
    {
        await using var command = database.Source.CreateCommand(sql);
        foreach (var value in values) command.Parameters.AddWithValue(value);
        await command.ExecuteNonQueryAsync();
    }

    private async Task<Guid> Seed()
    {
        var order = Guid.NewGuid().ToString();
        await Sql("INSERT INTO public.ordering_lab_orders(order_id) VALUES ($1)", order);
        var result = await new PostgresOrderConfirmationStore(database.Source).ConfirmAsync(order, Token);
        Assert.Equal(OrderConfirmationStatus.Confirmed, result.Status);
        return result.EventId!.Value;
    }

    private async Task<AcquiredOutboxMessage> Acquire(string owner = "worker")
    {
        var result = await Store.TryAcquireAsync(owner, Duration, Token);
        Assert.Equal(OutboxAcquisitionStatus.Acquired, result.Status);
        return Assert.IsType<AcquiredOutboxMessage>(result.Work);
    }

    private async Task<OutboxMessage> ReadContent(Guid id)
    {
        await using var command = database.Source.CreateCommand("""
            SELECT event_id, event_type, aggregate_id, payload::text, created_at_utc
            FROM public.ordering_outbox_messages WHERE event_id = $1
            """);
        command.Parameters.AddWithValue(id);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        return new(reader.GetGuid(0), reader.GetString(1), reader.GetString(2), reader.GetString(3),
            new DateTimeOffset(reader.GetDateTime(4)));
    }

    private async Task<string> Snapshot(Guid id)
    {
        await using var command = database.Source.CreateCommand("SELECT row_to_json(o)::text FROM public.ordering_outbox_messages o WHERE event_id = $1");
        command.Parameters.AddWithValue(id);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    private Task Expire(Guid id) => Sql("UPDATE public.ordering_outbox_messages SET lease_expires_at_utc = clock_timestamp() - interval '1 second' WHERE event_id = $1", id);

    private static Task<OutboxMutationResult> Mutate(PostgresOutboxProcessingStore store, OutboxLease lease, string operation, CancellationToken ct) => operation switch
    {
        "renew" => store.RenewAsync(lease, TimeSpan.FromMinutes(2), ct),
        "publish" => store.MarkPublishedAsync(lease, ct),
        "retry" => store.ScheduleRetryAsync(lease, TimeSpan.FromHours(1), ct),
        _ => throw new ArgumentOutOfRangeException(nameof(operation))
    };

    [Fact]
    public async Task PendingEventWithFutureCreationTime_IsImmediatelyEligible()
    {
        var id = await Seed();
        await Sql("UPDATE public.ordering_outbox_messages SET created_at_utc = clock_timestamp() + interval '1 day' WHERE event_id = $1", id);
        var content = await ReadContent(id);
        Assert.Equal(content, (await Acquire()).Content);
    }

    [Fact]
    public async Task Acquisition_DoesNotExposeWorkBeforeCommitConfirmation()
    {
        var id = await Seed();
        var before = await Snapshot(id);
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var store = new PostgresOutboxProcessingStore(database.Source, new(BeforeCommit: async (_, _, ct) =>
        {
            reached.SetResult();
            await release.Task.WaitAsync(ct);
        }));
        var pending = store.TryAcquireAsync("worker", Duration, timeout.Token);
        try
        {
            await reached.Task.WaitAsync(timeout.Token);
            Assert.Equal(before, await Snapshot(id));
            Assert.False(pending.IsCompleted);
        }
        finally { release.TrySetResult(); await pending; }
        Assert.Equal(OutboxAcquisitionStatus.Acquired, (await pending).Status);
        Assert.Equal(id, (await pending).Work!.Lease.EventId);
    }

    [Fact]
    public async Task PendingAcquisition_PreservesEntireEvent_AndReleasesTransactionLocks()
    {
        var id = await Seed();
        var before = await ReadContent(id);
        var acquired = await Acquire();
        Assert.Equal(before, acquired.Content);
        Assert.Equal(id, acquired.Lease.EventId);
        Assert.Equal("worker", acquired.Lease.Owner);
        Assert.Equal(1, acquired.Lease.Generation);
        await using var check = database.Source.CreateCommand("""
            SELECT publication_status = 'Processing' AND lease_owner = 'worker' AND lease_generation = 1
                AND lease_expires_at_utc = $2 AND lease_expires_at_utc > clock_timestamp()
                AND lease_expires_at_utc <= clock_timestamp() + interval '1 minute'
                AND next_attempt_at_utc IS NULL AND published_at_utc IS NULL
            FROM public.ordering_outbox_messages WHERE event_id = $1
            """);
        check.Parameters.AddWithValue(id);
        check.Parameters.AddWithValue(acquired.ExpiresAtUtc);
        Assert.Equal(true, await check.ExecuteScalarAsync());
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using var rowLock = new NpgsqlCommand("SELECT event_id FROM public.ordering_outbox_messages WHERE event_id = $1 FOR UPDATE NOWAIT", connection, transaction);
        rowLock.Parameters.AddWithValue(id);
        Assert.Equal(id, await rowLock.ExecuteScalarAsync());
    }

    [Fact]
    public async Task ConcurrentInstances_SkipActuallyLockedWork_AndAcquireDifferentEvents()
    {
        var firstId = await Seed();
        var secondId = await Seed();
        var reached = new TaskCompletionSource<Guid>(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await using var firstSource = NpgsqlDataSource.Create(database.ConnectionString);
        await using var secondSource = NpgsqlDataSource.Create(database.ConnectionString);
        var first = new PostgresOutboxProcessingStore(firstSource, new(AfterLock: async (c, tx, ct) =>
        {
            await using var check = new NpgsqlCommand("""
                SELECT event_id FROM public.ordering_outbox_messages
                WHERE event_id = ANY(ARRAY[$1, $2]) ORDER BY created_at_utc, event_id LIMIT 1
                """, c, tx);
            check.Parameters.AddWithValue(firstId);
            check.Parameters.AddWithValue(secondId);
            reached.SetResult((Guid)(await check.ExecuteScalarAsync(ct))!);
            await release.Task.WaitAsync(ct);
        }));
        var pending = first.TryAcquireAsync("first", Duration, timeout.Token);
        OutboxAcquisitionResult second;
        Guid lockedId;
        try
        {
            lockedId = await reached.Task.WaitAsync(timeout.Token);
            // A NOWAIT failure proves a real server row lock, not merely a pending client Task.
            await using (var connection = await database.Source.OpenConnectionAsync(timeout.Token))
            await using (var tx = await connection.BeginTransactionAsync(timeout.Token))
            await using (var probe = new NpgsqlCommand("SELECT event_id FROM public.ordering_outbox_messages WHERE event_id = $1 FOR UPDATE NOWAIT", connection, tx))
            {
                probe.Parameters.AddWithValue(lockedId);
                var error = await Assert.ThrowsAsync<PostgresException>(() => probe.ExecuteScalarAsync(timeout.Token));
                Assert.Equal(PostgresErrorCodes.LockNotAvailable, error.SqlState);
            }
            second = await new PostgresOutboxProcessingStore(secondSource).TryAcquireAsync("second", Duration, timeout.Token);
            Assert.Equal(OutboxAcquisitionStatus.Acquired, second.Status);
            Assert.NotEqual(lockedId, second.Work!.Lease.EventId);
            var unavailable = await Store.TryAcquireAsync("third", Duration, timeout.Token);
            Assert.Equal(OutboxAcquisitionStatus.NoEligibleWork, unavailable.Status);
            Assert.Null(unavailable.Work); // The Outbox still contains the locked Pending event.
        }
        finally { release.TrySetResult(); await pending; }
        var result = await pending;
        Assert.Equal(OutboxAcquisitionStatus.Acquired, result.Status);
        Assert.Equal(lockedId, result.Work!.Lease.EventId);
        Assert.NotEqual(result.Work.Lease.EventId, second.Work!.Lease.EventId);
    }

    [Fact]
    public async Task ManyIndependentInstances_CannotAcquireTheSameGeneration()
    {
        var id = await Seed();
        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(async i =>
        {
            await using var source = NpgsqlDataSource.Create(database.ConnectionString);
            return await new PostgresOutboxProcessingStore(source).TryAcquireAsync("worker-" + i, Duration, Token);
        }));
        var winner = Assert.Single(results, r => r.Status == OutboxAcquisitionStatus.Acquired);
        Assert.Equal(id, winner.Work!.Lease.EventId);
        Assert.Equal(1, winner.Work.Lease.Generation);
        Assert.Equal(15, results.Count(r => r.Status == OutboxAcquisitionStatus.NoEligibleWork && r.Work is null));
    }

    [Fact]
    public async Task ExpiredLeaseRecovery_IncrementsGeneration_AndFencesEveryOldMutationWithSameOwner()
    {
        var id = await Seed();
        var original = await ReadContent(id);
        var old = await Acquire();
        await Expire(id);
        var current = await Acquire();
        Assert.Equal(old.Lease.Generation + 1, current.Lease.Generation);
        Assert.Equal(original, current.Content);
        var before = await Snapshot(id);
        foreach (var operation in new[] { "renew", "publish", "retry" })
            Assert.Equal(OutboxMutationStatus.LeaseLost, (await Mutate(Store, old.Lease, operation, Token)).Status);
        Assert.Equal(before, await Snapshot(id));
        Assert.Equal(OutboxMutationStatus.Succeeded, (await Store.MarkPublishedAsync(current.Lease, Token)).Status);
        Assert.Equal(original, await ReadContent(id));
    }

    [Fact]
    public async Task ExpiredLeaseWithoutSuccessor_RejectsAllMutations()
    {
        var id = await Seed();
        var work = await Acquire();
        await Expire(id);
        var before = await Snapshot(id);
        foreach (var operation in new[] { "renew", "publish", "retry" })
            Assert.Equal(OutboxMutationStatus.LeaseLost, (await Mutate(Store, work.Lease, operation, Token)).Status);
        Assert.Equal(before, await Snapshot(id));
    }

    [Fact]
    public async Task IncorrectOwnerGenerationOrEvent_RejectsEveryMutation()
    {
        var id = await Seed();
        var work = await Acquire();
        var before = await Snapshot(id);
        foreach (var lease in new[]
        {
            new OutboxLease(id, "wrong-owner", work.Lease.Generation),
            new OutboxLease(id, work.Lease.Owner, work.Lease.Generation + 1),
            new OutboxLease(Guid.NewGuid(), work.Lease.Owner, work.Lease.Generation)
        })
        foreach (var operation in new[] { "renew", "publish", "retry" })
            Assert.Equal(OutboxMutationStatus.LeaseLost, (await Mutate(Store, lease, operation, Token)).Status);
        Assert.Equal(before, await Snapshot(id));
    }

    [Fact]
    public async Task Renewal_PreservesGenerationAndContent_AndDoesNotShortenLease()
    {
        var id = await Seed();
        var work = await Acquire();
        var shorter = await Store.RenewAsync(work.Lease, TimeSpan.FromSeconds(1), Token);
        Assert.Equal(OutboxMutationStatus.Succeeded, shorter.Status);
        Assert.Equal(work.ExpiresAtUtc, shorter.ExpiresAtUtc);
        var longer = await Store.RenewAsync(work.Lease, TimeSpan.FromMinutes(2), Token);
        Assert.Equal(OutboxMutationStatus.Succeeded, longer.Status);
        Assert.True(longer.ExpiresAtUtc > work.ExpiresAtUtc);
        Assert.Equal(work.Content, await ReadContent(id));
        Assert.Equal(OutboxMutationStatus.Succeeded, (await Store.MarkPublishedAsync(work.Lease, Token)).Status);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task RetryScheduling_ReleasesAuthority_AndIsAcquiredOnlyWhenDue(bool immediate)
    {
        var id = await Seed();
        var work = await Acquire();
        var result = await Store.ScheduleRetryAsync(work.Lease, immediate ? TimeSpan.Zero : TimeSpan.FromHours(1), Token);
        Assert.Equal(OutboxMutationStatus.Succeeded, result.Status);
        Assert.Null(result.ExpiresAtUtc);
        await using var check = database.Source.CreateCommand("""
            SELECT publication_status = 'RetryScheduled' AND lease_owner IS NULL AND lease_expires_at_utc IS NULL
                AND lease_generation = $2 AND published_at_utc IS NULL
                AND next_attempt_at_utc <= clock_timestamp() + interval '1 hour'
                AND ($3 OR next_attempt_at_utc > clock_timestamp())
            FROM public.ordering_outbox_messages WHERE event_id = $1
            """);
        check.Parameters.AddWithValue(id);
        check.Parameters.AddWithValue(work.Lease.Generation);
        check.Parameters.AddWithValue(immediate);
        Assert.Equal(true, await check.ExecuteScalarAsync());
        Assert.Equal(OutboxMutationStatus.LeaseLost, (await Store.MarkPublishedAsync(work.Lease, Token)).Status);
        if (!immediate)
        {
            Assert.Equal(OutboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status);
            await Sql("UPDATE public.ordering_outbox_messages SET next_attempt_at_utc = clock_timestamp() - interval '1 second' WHERE event_id = $1", id);
        }
        var retried = await Acquire("other");
        Assert.Equal(work.Lease.Generation + 1, retried.Lease.Generation);
        Assert.Equal(work.Content, retried.Content);
        await using var cleared = database.Source.CreateCommand("SELECT next_attempt_at_utc IS NULL FROM public.ordering_outbox_messages WHERE event_id = $1");
        cleared.Parameters.AddWithValue(id);
        Assert.Equal(true, await cleared.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Published_IsTerminal_AndRecordsLocalCompletionWithoutChangingEvent()
    {
        var id = await Seed();
        var work = await Acquire();
        Assert.Equal(OutboxMutationStatus.Succeeded, (await Store.MarkPublishedAsync(work.Lease, Token)).Status);
        await using var check = database.Source.CreateCommand("""
            SELECT publication_status = 'Published' AND published_at_utc IS NOT NULL
                AND published_at_utc <= clock_timestamp() AND published_at_utc >= created_at_utc
                AND lease_owner IS NULL AND lease_expires_at_utc IS NULL AND next_attempt_at_utc IS NULL
                AND lease_generation = $2 FROM public.ordering_outbox_messages WHERE event_id = $1
            """);
        check.Parameters.AddWithValue(id);
        check.Parameters.AddWithValue(work.Lease.Generation);
        Assert.Equal(true, await check.ExecuteScalarAsync());
        var before = await Snapshot(id);
        foreach (var operation in new[] { "renew", "publish", "retry" })
            Assert.Equal(OutboxMutationStatus.LeaseLost, (await Mutate(Store, work.Lease, operation, Token)).Status);
        Assert.Equal(OutboxAcquisitionStatus.NoEligibleWork, (await Store.TryAcquireAsync("other", Duration, Token)).Status);
        Assert.Equal(before, await Snapshot(id));
        Assert.Equal(work.Content, await ReadContent(id));
    }

    [Theory]
    [InlineData("renew")]
    [InlineData("publish")]
    [InlineData("retry")]
    public async Task Mutation_ValidatesFreshDatabaseTimeAfterProvenServerLockWait(string operation)
    {
        var id = await Seed();
        var work = await Acquire();
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await using var blocker = await database.Source.OpenConnectionAsync(timeout.Token);
        await using var transaction = await blocker.BeginTransactionAsync(timeout.Token);
        await using var locked = new NpgsqlCommand("SELECT event_id FROM public.ordering_outbox_messages WHERE event_id = $1 FOR UPDATE", blocker, transaction);
        locked.Parameters.AddWithValue(id);
        await locked.ExecuteScalarAsync(timeout.Token);
        var started = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new PostgresOutboxProcessingStore(database.Source, new(BeforeLock: async (c, tx, ct) =>
        {
            await using var settings = new NpgsqlCommand("SET LOCAL lock_timeout = '8s'; SET LOCAL statement_timeout = '9s'", c, tx);
            await settings.ExecuteNonQueryAsync(ct);
            started.SetResult(c.ProcessID);
        }));
        var pending = Mutate(store, work.Lease, operation, timeout.Token);
        var committed = false;
        try
        {
            await WaitForBlocked(await started.Task.WaitAsync(timeout.Token), blocker.ProcessID, timeout.Token);
            // Expiry is strictly after the contender's transaction/lock statement began, but before
            // its lock is granted. A pre-wait now()/statement_timestamp() would authorize it wrongly.
            await using var expire = new NpgsqlCommand("UPDATE public.ordering_outbox_messages SET lease_expires_at_utc = clock_timestamp() WHERE event_id = $1", blocker, transaction);
            expire.Parameters.AddWithValue(id);
            await expire.ExecuteNonQueryAsync(timeout.Token);
            await transaction.CommitAsync(timeout.Token);
            committed = true;
        }
        finally
        {
            if (!committed) await transaction.RollbackAsync();
            await pending;
        }
        Assert.Equal(OutboxMutationStatus.LeaseLost, (await pending).Status);
        Assert.Equal(work.Content, await ReadContent(id));
    }

    private async Task WaitForBlocked(int contender, int blocker, CancellationToken ct)
    {
        await using var command = database.Source.CreateCommand("""
            SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                WHERE pid = $1 AND wait_event_type = 'Lock' AND $2 = ANY(pg_blocking_pids(pid)))
            """);
        command.Parameters.AddWithValue(contender);
        command.Parameters.AddWithValue(blocker);
        while (!(bool)(await command.ExecuteScalarAsync(ct))!)
            await Task.Delay(10, ct); // Poll server evidence, never elapsed time as proof of blocking.
    }

    [Theory]
    [InlineData("acquire")]
    [InlineData("renew")]
    [InlineData("publish")]
    [InlineData("retry")]
    public async Task FailureBeforeCommit_RollsBackWholeOperation(string operation)
    {
        var id = await Seed();
        var work = operation == "acquire" ? null : await Acquire();
        var before = await Snapshot(id);
        var store = new PostgresOutboxProcessingStore(database.Source, new(BeforeCommit: (_, _, _) => throw new IOException("Injected before commit")));
        if (work is null)
            await Assert.ThrowsAsync<IOException>(() => store.TryAcquireAsync("failed", Duration, Token));
        else
            await Assert.ThrowsAsync<IOException>(() => Mutate(store, work.Lease, operation, Token));
        Assert.Equal(before, await Snapshot(id));
        if (work is null) Assert.Equal(1, (await Acquire()).Lease.Generation);
        else Assert.Equal(OutboxMutationStatus.Succeeded, (await Store.MarkPublishedAsync(work.Lease, Token)).Status);
    }

    [Theory]
    [InlineData("acquire")]
    [InlineData("renew")]
    [InlineData("publish")]
    [InlineData("retry")]
    public async Task CancellationBeforeCommit_PropagatesAndRollsBack(string operation)
    {
        var id = await Seed();
        var work = operation == "acquire" ? null : await Acquire();
        var before = await Snapshot(id);
        using var cancellation = new CancellationTokenSource();
        var store = new PostgresOutboxProcessingStore(database.Source, new(BeforeCommit: (_, _, ct) =>
        {
            cancellation.Cancel();
            ct.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }));
        if (work is null)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => store.TryAcquireAsync("worker", Duration, cancellation.Token));
        else
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Mutate(store, work.Lease, operation, cancellation.Token));
        Assert.Equal(before, await Snapshot(id));
    }

    [Fact]
    public async Task TransientProviderFailureBeforeCommit_UsesApplicationExceptionAndRollsBack()
    {
        var id = await Seed();
        var before = await Snapshot(id);
        var store = new PostgresOutboxProcessingStore(database.Source, new(BeforeCommit: (_, _, _) =>
            throw new NpgsqlException("Injected connection failure", new IOException())));
        await Assert.ThrowsAsync<OutboxStorageUnavailableException>(() => store.TryAcquireAsync("worker", Duration, Token));
        Assert.Equal(before, await Snapshot(id));
    }

    [Fact]
    public async Task CancellationDuringProvenLockWait_ReleasesTransactionWithoutMutation()
    {
        var id = await Seed();
        var work = await Acquire();
        var before = await Snapshot(id);
        await using var blocker = await database.Source.OpenConnectionAsync();
        await using var tx = await blocker.BeginTransactionAsync();
        await using var command = new NpgsqlCommand("SELECT event_id FROM public.ordering_outbox_messages WHERE event_id = $1 FOR UPDATE", blocker, tx);
        command.Parameters.AddWithValue(id);
        await command.ExecuteScalarAsync();
        var started = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var store = new PostgresOutboxProcessingStore(database.Source, new(BeforeLock: (c, _, _) =>
        {
            started.SetResult(c.ProcessID);
            return Task.CompletedTask;
        }));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        var pending = store.MarkPublishedAsync(work.Lease, cancellation.Token);
        try
        {
            await WaitForBlocked(await started.Task.WaitAsync(cancellation.Token), blocker.ProcessID, cancellation.Token);
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        }
        finally { await tx.RollbackAsync(); }
        Assert.Equal(before, await Snapshot(id));
        Assert.Equal(OutboxMutationStatus.Succeeded, (await Store.MarkPublishedAsync(work.Lease, Token)).Status);
    }

    public static TheoryData<string, bool, string> AmbiguousCommits
    {
        get
        {
            var cases = new TheoryData<string, bool, string>();
            foreach (var operation in new[] { "acquire", "renew", "publish", "retry" })
            foreach (var committed in new[] { false, true })
            foreach (var failure in new[] { "io", "timeout", "cancel", "provider" })
                cases.Add(operation, committed, failure);
            return cases;
        }
    }

    [Theory]
    [MemberData(nameof(AmbiguousCommits))]
    public async Task LostCommitResponse_IsUnknownAfterEitherActualCommitOrRollback(string operation, bool committed, string failure)
    {
        var id = await Seed();
        var content = await ReadContent(id);
        var work = operation == "acquire" ? null : await Acquire();
        var before = await Snapshot(id);
        var calls = 0;
        var store = new PostgresOutboxProcessingStore(database.Source, new(Commit: async (_, tx, ct) =>
        {
            calls++;
            if (committed) await tx.CommitAsync(ct); else await tx.RollbackAsync(ct);
            throw failure switch
            {
                "timeout" => new TimeoutException("Lost response"),
                "cancel" => new OperationCanceledException("Lost response"),
                "provider" => new NpgsqlException("Lost response", new IOException()),
                _ => new IOException("Lost response")
            };
        }));
        if (work is null)
        {
            var result = await store.TryAcquireAsync("uncertain", Duration, Token);
            Assert.Equal(OutboxAcquisitionStatus.OutcomeUnknown, result.Status);
            Assert.Null(result.Work);
        }
        else
        {
            var result = await Mutate(store, work.Lease, operation, Token);
            Assert.Equal(OutboxMutationStatus.OutcomeUnknown, result.Status);
            Assert.Null(result.ExpiresAtUtc);
        }
        Assert.Equal(1, calls); // No hidden retry.
        if (committed) Assert.NotEqual(before, await Snapshot(id));
        else Assert.Equal(before, await Snapshot(id));
        Assert.Equal(content, await ReadContent(id));
        if (work is null)
            Assert.Equal(committed ? OutboxAcquisitionStatus.NoEligibleWork : OutboxAcquisitionStatus.Acquired,
                (await Store.TryAcquireAsync("other", Duration, Token)).Status);
        else
            Assert.Equal(committed && operation != "renew" ? OutboxMutationStatus.LeaseLost : OutboxMutationStatus.Succeeded,
                (await Store.MarkPublishedAsync(work.Lease, Token)).Status);
    }

    [Fact]
    public async Task Acquisition_RevalidatesEligibilityAfterSelectionUnderLock()
    {
        var id = await Seed();
        await Acquire();
        await Expire(id);
        var before = await Snapshot(id);
        var store = new PostgresOutboxProcessingStore(database.Source, new(AfterLock: async (c, tx, ct) =>
        {
            await using var command = new NpgsqlCommand("UPDATE public.ordering_outbox_messages SET lease_expires_at_utc = clock_timestamp() + interval '1 hour' WHERE event_id = $1", c, tx);
            command.Parameters.AddWithValue(id);
            await command.ExecuteNonQueryAsync(ct);
        }));
        var result = await store.TryAcquireAsync("other", Duration, Token);
        Assert.Equal(OutboxAcquisitionStatus.NoEligibleWork, result.Status);
        Assert.Null(result.Work);
        Assert.Equal(before, await Snapshot(id)); // Failed revalidation also rolls back the injected write.
    }

    [Fact]
    public async Task GenerationExhaustion_FailsWithoutOverflowOrReusingAuthority()
    {
        var id = await Seed();
        await Acquire();
        await Expire(id);
        await Sql("UPDATE public.ordering_outbox_messages SET lease_generation = $2 WHERE event_id = $1", id, long.MaxValue);
        var before = await Snapshot(id);
        await Assert.ThrowsAsync<InvalidOperationException>(() => Store.TryAcquireAsync("other", Duration, Token));
        Assert.Equal(before, await Snapshot(id));
    }

    [Fact]
    public async Task AcquisitionIndex_SupportsActualOrderedDueRangeQuery()
    {
        await Seed();
        await using var connection = await database.Source.OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();
        await using (var settings = new NpgsqlCommand("SET LOCAL enable_seqscan = off", connection, transaction))
            await settings.ExecuteNonQueryAsync();
        await using var command = new NpgsqlCommand("EXPLAIN " + PostgresOutboxProcessingStore.AcquisitionSql, connection, transaction);
        await using var reader = await command.ExecuteReaderAsync();
        var lines = new List<string>();
        while (await reader.ReadAsync()) lines.Add(reader.GetString(0));
        var plan = string.Join('\n', lines);
        Assert.Contains("ordering_outbox_processing_due", plan);
        Assert.Contains("Index Cond", plan);
        Assert.DoesNotContain("Sort", plan); // Applicability only; not a production performance claim.
    }

    [Fact]
    public async Task ProvisioningIsRepeatable_AndConstraintsRejectIncoherentStatesAndDates()
    {
        var id = await Seed();
        var before = await Snapshot(id);
        await InboxSchema.ApplyAsync(database.Source);
        await InboxSchema.ApplyAsync(database.Source);
        foreach (var assignment in new[]
        {
            "publication_status = 'Unknown'", "publication_status = 'Processing'",
            "publication_status = 'RetryScheduled'", "publication_status = 'Published'",
            "lease_owner = 'orphan'", "lease_expires_at_utc = clock_timestamp()",
            "next_attempt_at_utc = clock_timestamp()", "published_at_utc = clock_timestamp()",
            "lease_generation = -1", "lease_generation = 1",
            "publication_status = 'Processing', lease_generation = 1, lease_owner = ' ', lease_expires_at_utc = clock_timestamp()",
            "publication_status = 'Processing', lease_generation = 1, lease_owner = 'worker', lease_expires_at_utc = 'infinity'",
            "publication_status = 'RetryScheduled', lease_generation = 1, next_attempt_at_utc = 'infinity'",
            "publication_status = 'Published', lease_generation = 1, published_at_utc = 'infinity'"
        })
        {
            var error = await Assert.ThrowsAsync<PostgresException>(() => Sql($"UPDATE public.ordering_outbox_messages SET {assignment} WHERE event_id = $1", id));
            Assert.Equal(PostgresErrorCodes.CheckViolation, error.SqlState);
        }
        Assert.Equal(before, await Snapshot(id));
    }

    [Fact]
    public async Task Coordination_OverridesAsynchronousSessionDefault_AndRequiresPersistentStorage()
    {
        var id = await Seed();
        var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Options = "-c synchronous_commit=off" };
        await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
        var calls = 0;
        var store = new PostgresOutboxProcessingStore(source, new(BeforeCommit: async (c, tx, ct) =>
        {
            await using var check = new NpgsqlCommand("SHOW synchronous_commit", c, tx);
            Assert.Equal("on", await check.ExecuteScalarAsync(ct));
            calls++;
        }));
        var work = (await store.TryAcquireAsync("worker", Duration, Token)).Work!;
        Assert.Equal(OutboxMutationStatus.Succeeded, (await store.RenewAsync(work.Lease, Duration, Token)).Status);
        Assert.Equal(OutboxMutationStatus.Succeeded, (await store.ScheduleRetryAsync(work.Lease, TimeSpan.Zero, Token)).Status);
        work = (await store.TryAcquireAsync("worker", Duration, Token)).Work!;
        Assert.Equal(OutboxMutationStatus.Succeeded, (await store.MarkPublishedAsync(work.Lease, Token)).Status);
        Assert.Equal(5, calls);
        await using var table = source.CreateCommand("SELECT relpersistence = 'p' FROM pg_class WHERE oid = 'public.ordering_outbox_messages'::regclass");
        Assert.Equal(true, await table.ExecuteScalarAsync());
        Assert.Equal(id, work.Lease.EventId);
    }

    [Fact]
    public async Task VersionThreeUpgrade_PreservesEventInboxAndExistingMigrationHistory()
    {
        var name = "outbox_processing_upgrade_" + Guid.NewGuid().ToString("N");
        await Sql($"CREATE DATABASE {name}");
        try
        {
            var builder = new NpgsqlConnectionStringBuilder(database.ConnectionString) { Database = name };
            await using var source = NpgsqlDataSource.Create(builder.ConnectionString);
            foreach (var script in new[] { "001_inbox.sql", "002_inbox_processing.sql", "003_outbox_lab.sql" })
            {
                using var stream = typeof(InboxSchema).Assembly.GetManifestResourceStream("Ordering.Infrastructure.PostgreSql.Migrations." + script)!;
                using var reader = new StreamReader(stream);
                await using var migration = source.CreateCommand(await reader.ReadToEndAsync());
                await migration.ExecuteNonQueryAsync();
            }
            await using (var setup = source.CreateCommand("""
                CREATE TABLE public.ordering_inbox_schema_version(version integer PRIMARY KEY,
                    applied_at_utc timestamptz NOT NULL DEFAULT clock_timestamp());
                INSERT INTO public.ordering_inbox_schema_version(version) VALUES (1), (2), (3);
                INSERT INTO public.ordering_lab_orders(order_id) VALUES ('existing-order');
                INSERT INTO public.ordering_inbox
                    (inbox_id, partner_id, integration_scope, event_id, order_id,
                     original_payload, normalized_payload, normalized_version)
                VALUES (gen_random_uuid(), 'partner', 'scope', 'event', 'order',
                    decode('00ff0a', 'hex'), '{"EventId":"event","OrderId":"order"}', 1);
                """)) await setup.ExecuteNonQueryAsync();
            var eventId = (await new PostgresOrderConfirmationStore(source).ConfirmAsync("existing-order", Token)).EventId!.Value;
            await using var eventData = source.CreateCommand("""
                SELECT json_build_array(event_id, event_type, aggregate_id, payload, created_at_utc)::text
                FROM public.ordering_outbox_messages
                """);
            await using var inbox = source.CreateCommand("SELECT row_to_json(i)::text FROM public.ordering_inbox i");
            await using var history = source.CreateCommand("SELECT json_agg(v ORDER BY version)::text FROM public.ordering_inbox_schema_version v WHERE version <= 3");
            var originalEvent = await eventData.ExecuteScalarAsync();
            var originalInbox = await inbox.ExecuteScalarAsync();
            var originalHistory = await history.ExecuteScalarAsync();
            await InboxSchema.ApplyAsync(source);
            await using var fullHistory = source.CreateCommand("SELECT json_agg(v ORDER BY version)::text FROM public.ordering_inbox_schema_version v");
            var appliedHistory = await fullHistory.ExecuteScalarAsync();
            await InboxSchema.ApplyAsync(source);
            Assert.Equal(appliedHistory, await fullHistory.ExecuteScalarAsync());
            Assert.Equal(originalEvent, await eventData.ExecuteScalarAsync());
            Assert.Equal(originalInbox, await inbox.ExecuteScalarAsync());
            Assert.Equal(originalHistory, await history.ExecuteScalarAsync());
            await using var versions = source.CreateCommand("SELECT array_agg(version ORDER BY version) FROM public.ordering_inbox_schema_version");
            Assert.Equal(new[] { 1, 2, 3, 4 }, (int[])(await versions.ExecuteScalarAsync())!);
            await using var defaults = source.CreateCommand("""
                SELECT publication_status = 'Pending' AND lease_generation = 0 AND lease_owner IS NULL
                    AND lease_expires_at_utc IS NULL AND next_attempt_at_utc IS NULL AND published_at_utc IS NULL
                FROM public.ordering_outbox_messages
                """);
            Assert.Equal(true, await defaults.ExecuteScalarAsync());
            var acquired = await new PostgresOutboxProcessingStore(source).TryAcquireAsync("new-worker", Duration, Token);
            Assert.Equal(OutboxAcquisitionStatus.Acquired, acquired.Status);
            Assert.Equal(eventId, acquired.Work!.Lease.EventId);
            Assert.Equal(1, acquired.Work.Lease.Generation);
            Assert.Equal(originalEvent, await eventData.ExecuteScalarAsync());
        }
        finally { await Sql($"DROP DATABASE {name} WITH (FORCE)"); }
    }
}
