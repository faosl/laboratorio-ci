using System.Net;
using System.Net.Http.Headers;
using System.Security.Cryptography;
using System.Text;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Ordering.API;
using Ordering.API.LaboratoryHost;
using Ordering.Application.Inbox;
using Ordering.Contracts.Partners;
using Ordering.Infrastructure.PostgreSql;

namespace Ordering.Infrastructure.PostgreSql.Tests;

public sealed class UberEatsInboxHttpTests(DatabaseFixture database) : IClassFixture<DatabaseFixture>
{
    private const string Secret = "synthetic-http-inbox-secret";
    private static byte[] Body(string id, string order = "order") => Encoding.UTF8.GetBytes(
        $"{{\"event_type\":\"orders.notification\",\"event_id\":\"{id}\",\"meta\":{{\"resource_id\":\"{order}\"}},\"extra\":\"café\"}}\r\n");
    private static HttpRequestMessage Request(byte[] body, bool validSignature = true)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, UberEatsOrderEndpoint.Route)
        { Content = new ByteArrayContent(body) };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");
        request.Headers.Add("X-Uber-Signature", validSignature
            ? Convert.ToHexString(HMACSHA256.HashData(Encoding.UTF8.GetBytes(Secret), body)).ToLowerInvariant() : new string('0', 64));
        return request;
    }

    private WebApplicationFactory<Program> Host(string scope = "http-lab", RegistrationFaults? faults = null,
        Action? registering = null, string environment = "Laboratory") =>
        new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment(environment);
            builder.ConfigureServices(services =>
            {
                var configuration = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["Inbox:ConnectionString"] = database.ConnectionString,
                    ["UberEats:IntegrationScope"] = scope,
                    ["UberEats:SharedSecret"] = Secret
                }).Build();
                LaboratoryComposition.Configure(services, configuration);
                services.AddSingleton<IInboxStore>(provider => new ObservedStore(
                    new PostgresInboxStore(provider.GetRequiredService<NpgsqlDataSource>(), faults), registering));
            });
        });

    private static async Task Send(HttpClient client, byte[] body, int expected, bool validSignature = true)
    {
        using var request = Request(body, validSignature);
        using var response = await client.SendAsync(request);
        Assert.Equal(expected, (int)response.StatusCode);
        var text = await response.Content.ReadAsStringAsync();
        if (expected == 200) Assert.Empty(text);
        Assert.DoesNotContain(Secret, text);
        if (body.Length > 10) Assert.DoesNotContain(Encoding.UTF8.GetString(body), text, StringComparison.Ordinal);
        Assert.DoesNotContain("synthetic-private-failure", text);
    }

    private Task<InboxEntry?> Read(string id, string scope = "http-lab", string partner = "uber-eats") =>
        new PostgresInboxStore(database.Source).GetByKeyAsync(new(partner, scope, id), CancellationToken.None);

    [Fact]
    public async Task DurableReceipt_RoundTripRedeliveryAndConflict()
    {
        using var host = Host(); using var client = host.CreateClient();
        var id = Guid.NewGuid().ToString(); var body = Body(id);
        await Send(client, body, 200);
        var first = Assert.IsType<InboxEntry>(await Read(id));
        Assert.Equal(body, first.Content.OriginalPayload);
        Assert.Equal(new InboxKey("uber-eats", "http-lab", id), first.Content.Key);
        Assert.Equal(id, first.Content.Notification.EventId);
        Assert.Equal("order", first.Content.Notification.OrderId);
        Assert.Equal(1, first.Content.NormalizedVersion);
        Assert.Equal("Pending", first.State);
        await Send(client, body, 200);
        await Send(client, Body(id, "other"), 409);
        await Send(client, body.Concat(new byte[] { 32 }).ToArray(), 409);
        var after = Assert.IsType<InboxEntry>(await Read(id));
        Assert.Equal(first.InboxId, after.InboxId);
        Assert.Equal(first.ReceivedAtUtc, after.ReceivedAtUtc);
        Assert.Equal(first.State, after.State);
        Assert.Equal(first.Content.OriginalPayload, after.Content.OriginalPayload);
    }

    [Fact]
    public async Task HttpResponse_WaitsForCommit()
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var host = Host(faults: new(BeforeCommit: async (connection, transaction, ct) =>
        {
            await using var settings = new NpgsqlCommand("SHOW synchronous_commit", connection, transaction);
            Assert.Equal("on", await settings.ExecuteScalarAsync(ct));
            reached.TrySetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
        }));
        using var client = host.CreateClient();
        var id = Guid.NewGuid().ToString();
        var response = Send(client, Body(id), 200);
        try
        {
            await reached.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.False(response.IsCompleted);
            Assert.Null(await Read(id));
        }
        finally { release.TrySetResult(); }
        await response;
        Assert.NotNull(await Read(id));
    }

    [Theory]
    [InlineData("Processing")]
    [InlineData("RetryScheduled")]
    [InlineData("Completed")]
    public async Task Redelivery_PreservesProcessingMetadataAndDurableAcknowledgment(string state)
    {
        using var host = Host(); using var client = host.CreateClient();
        var id = Guid.NewGuid().ToString(); var body = Body(id);
        await Send(client, body, 200);
        var entry = (await Read(id))!;
        await using (var prioritize = database.Source.CreateCommand("UPDATE public.ordering_inbox SET received_at_utc = '1900-01-01Z' WHERE inbox_id = $1"))
        {
            prioritize.Parameters.AddWithValue(entry.InboxId);
            await prioritize.ExecuteNonQueryAsync();
        }
        var processing = new PostgresInboxProcessingStore(database.Source);
        var acquired = await processing.TryAcquireAsync("http-regression", TimeSpan.FromMinutes(1), CancellationToken.None);
        Assert.Equal(entry.InboxId, acquired.Work!.Lease.InboxId);
        if (state == "RetryScheduled")
            Assert.Equal(InboxMutationStatus.Succeeded, (await processing.ScheduleRetryAsync(acquired.Work.Lease, TimeSpan.FromDays(1), CancellationToken.None)).Status);
        if (state == "Completed")
            Assert.Equal(InboxMutationStatus.Succeeded, (await processing.CompleteAsync(acquired.Work.Lease, CancellationToken.None)).Status);
        async Task<string> Snapshot()
        {
            await using var command = database.Source.CreateCommand("SELECT row_to_json(i)::text FROM public.ordering_inbox i WHERE inbox_id = $1");
            command.Parameters.AddWithValue(entry.InboxId);
            return (string)(await command.ExecuteScalarAsync())!;
        }
        var before = await Snapshot();
        await Send(client, body, 200);
        await Send(client, Body(id, "conflict"), 409);
        Assert.Equal(before, await Snapshot());
        Assert.Equal(state, (await Read(id))!.State);
    }

    [Fact]
    public async Task ConcurrentIndependentHosts_CreateOneDurableRecord()
    {
        var id = Guid.NewGuid().ToString(); var body = Body(id);
        var hosts = Enumerable.Range(0, 24).Select(_ => Host()).ToArray();
        try
        {
            await Task.WhenAll(hosts.Select(async host =>
            { using var client = host.CreateClient(); await Send(client, body, 200); }));
            await using var count = database.Source.CreateCommand("SELECT count(*) FROM public.ordering_inbox WHERE event_id = $1");
            count.Parameters.AddWithValue(id);
            Assert.Equal(1L, await count.ExecuteScalarAsync());
            Assert.NotNull(await Read(id));
        }
        finally { foreach (var host in hosts) host.Dispose(); }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task LostCommitConfirmation_Is503_ThenRedeliveryResolves(bool commit)
    {
        var id = Guid.NewGuid().ToString(); var body = Body(id);
        using (var host = Host(faults: new(Commit: async (_, tx, ct) =>
        {
            if (commit) await tx.CommitAsync(ct); else await tx.RollbackAsync(ct);
            throw new IOException("synthetic-private-failure");
        })))
        {
            using var client = host.CreateClient();
            await Send(client, body, 503);
        }
        var before = await Read(id);
        Assert.Equal(commit, before is not null);
        using var healthy = Host(); using var retry = healthy.CreateClient();
        await Send(retry, body, 200);
        var after = Assert.IsType<InboxEntry>(await Read(id));
        if (commit) Assert.Equal(before!.InboxId, after.InboxId);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task FailureBeforeDurableConfirmation_NeverAcknowledges(bool duplicate)
    {
        var id = Guid.NewGuid().ToString(); var body = Body(id);
        if (duplicate)
        { using var healthy = Host(); using var client = healthy.CreateClient(); await Send(client, body, 200); }
        using var host = Host(faults: new(BeforeCommit: (_, _, _) => throw new IOException("synthetic-private-failure")));
        using var failed = host.CreateClient();
        await Send(failed, body, 503);
        Assert.Equal(duplicate, await Read(id) is not null);
    }

    [Fact]
    public async Task DuplicateCommitBarrierFailure_IsNotAcknowledged()
    {
        var body = Body(Guid.NewGuid().ToString());
        using (var healthy = Host())
        { using var client = healthy.CreateClient(); await Send(client, body, 200); }
        using var host = Host(faults: new(Commit: async (_, tx, ct) =>
        { await tx.RollbackAsync(ct); throw new TimeoutException("synthetic-private-failure"); }));
        using var failed = host.CreateClient();
        await Send(failed, body, 503);
    }

    [Theory]
    [InlineData(true, 503)]
    [InlineData(false, 500)]
    public async Task ProviderTransientAndUnexpectedFailures_AreDistinguished(bool transient, int status)
    {
        using var host = Host(faults: new(BeforeCommit: (_, _, _) => throw (transient
            ? new NpgsqlException("synthetic-private-failure", new IOException())
            : new InvalidOperationException("synthetic-private-failure"))));
        using var client = host.CreateClient();
        var id = Guid.NewGuid().ToString();
        await Send(client, Body(id), status);
        Assert.Null(await Read(id));
    }

    [Theory]
    [InlineData("Production")]
    [InlineData("Development")]
    public async Task ConfiguredInbox_DoesNotExposeRouteOutsideLaboratory(string environment)
    {
        var calls = 0;
        using var host = Host(registering: () => calls++, environment: environment);
        using var client = host.CreateClient();
        await Send(client, Body(Guid.NewGuid().ToString()), 404);
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task PendingCommit_DoesNotAllowPrematureAcknowledgementOrAbsentRollbackInference()
    {
        var reached = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var host = Host(faults: new(Commit: async (_, tx, ct) =>
        {
            reached.TrySetResult();
            await release.Task.WaitAsync(TimeSpan.FromSeconds(20), ct);
            await tx.CommitAsync(ct);
            throw new IOException("synthetic-private-failure");
        }));
        using var client = host.CreateClient();
        var id = Guid.NewGuid().ToString(); var body = Body(id);
        var pending = Send(client, body, 503);
        try
        {
            await reached.Task.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.False(pending.IsCompleted);
            Assert.Null(await Read(id));
        }
        finally { release.TrySetResult(); }
        await pending;
        using var healthy = Host(); using var retry = healthy.CreateClient();
        await Send(retry, body, 200);
    }

    [Fact]
    public async Task InvalidInput_NeverReachesPersistence()
    {
        var calls = 0;
        using var host = Host(registering: () => Interlocked.Increment(ref calls));
        using var client = host.CreateClient();
        await Send(client, Body("invalid-hmac"), 403, false);
        await Send(client, [255], 400);
        await Send(client, Encoding.UTF8.GetBytes("{"), 400);
        await Send(client, Encoding.UTF8.GetBytes("{}"), 422);
        await Send(client, Body(new string('x', 201)), 422);
        await Send(client, Body("nul\\u0000event"), 422);
        await Send(client, new byte[1024 * 1024 + 1], 413);
        Assert.Equal(0, calls);
    }

    [Fact]
    public async Task ScopePartnerAndEventIsolation_OrderDoesNotDeduplicate()
    {
        var id = Guid.NewGuid().ToString();
        using var firstHost = Host("scope-a"); using var first = firstHost.CreateClient();
        using var secondHost = Host("scope-b"); using var second = secondHost.CreateClient();
        await Send(first, Body(id), 200); await Send(second, Body(id), 200);
        await Send(first, Body(id + "-2"), 200);
        var other = new InboxNotification(new("other-partner", "scope-a", id), new OrderNotification(id, "order"), 1, Body(id));
        Assert.Equal(InboxRegistrationStatus.Inserted,
            (await new PostgresInboxStore(database.Source).RegisterAsync(other, CancellationToken.None)).Status);
        var entries = new[] { await Read(id, "scope-a"), await Read(id, "scope-b"),
            await Read(id + "-2", "scope-a"), await Read(id, "scope-a", "other-partner") };
        Assert.All(entries, entry => Assert.NotNull(entry));
        Assert.Equal(4, entries.Select(entry => entry!.InboxId).Distinct().Count());
    }

    [Fact]
    public async Task LaboratoryExecutable_UsesActualComposition()
    {
        using var host = new WebApplicationFactory<LaboratoryProgram>().WithWebHostBuilder(builder =>
        {
            builder.UseEnvironment("Laboratory");
            builder.UseSetting("Inbox:ConnectionString", database.ConnectionString);
            builder.UseSetting("UberEats:IntegrationScope", "executable-lab");
            builder.UseSetting("UberEats:SharedSecret", Secret);
        });
        using var client = host.CreateClient();
        var id = Guid.NewGuid().ToString();
        await Send(client, Body(id), 200);
        Assert.NotNull(await Read(id, "executable-lab"));
    }

    [Theory]
    [InlineData("Inbox:ConnectionString")]
    [InlineData("UberEats:IntegrationScope")]
    [InlineData("UberEats:SharedSecret")]
    public void MissingConfiguration_FailsSafely(string missing)
    {
        var values = new Dictionary<string, string?> { ["Inbox:ConnectionString"] = database.ConnectionString,
            ["UberEats:IntegrationScope"] = "lab", ["UberEats:SharedSecret"] = Secret };
        values.Remove(missing);
        var services = new ServiceCollection();
        var error = Assert.Throws<InvalidOperationException>(() => LaboratoryComposition.Configure(services,
            new ConfigurationBuilder().AddInMemoryCollection(values).Build()));
        Assert.DoesNotContain(Secret, error.ToString());
        Assert.DoesNotContain(database.ConnectionString, error.ToString());
    }

    private sealed class ObservedStore(IInboxStore inner, Action? registering) : IInboxStore
    {
        public Task<InboxRegistrationResult> RegisterAsync(InboxNotification input, CancellationToken ct)
        { registering?.Invoke(); return inner.RegisterAsync(input, ct); }
        public Task<InboxEntry?> GetByKeyAsync(InboxKey key, CancellationToken ct) => inner.GetByKeyAsync(key, ct);
    }
}
