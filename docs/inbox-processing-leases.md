# Durable Inbox processing coordination

This increment adds storage coordination in .NET 8, not an executable worker or business workflow.
`IInboxStore` still owns authenticated durable registration and deduplication. The independent
Application port `IInboxProcessingStore` owns acquisition, renewal, completion and retry scheduling;
`PostgresInboxProcessingStore` implements it using Npgsql. Domain, normalized partner contracts,
HTTP responses and publication policies are unchanged.

## State and lease model

- `Pending`: eligible for initial acquisition.
- `Processing`: reserved, but authority also requires a currently unexpired lease.
- `RetryScheduled`: eligible when `next_attempt_at_utc` is due.
- `Completed`: excluded from automatic acquisition.

Acquisition changes Pending/due RetryScheduled/expired Processing to Processing. Renewal preserves
Processing. A valid owner can complete or schedule a retry, clearing owner and expiration.
There is no Expired state, sweeper, attempt counter, checkpoint or dead-letter workflow.

A lease identity is `(InboxId, Owner, Generation)`. Choose a new owner identifier for each worker
execution, including restarts. Owner is nonblank, at most 200 characters, without NUL. Generation
starts at zero, increases on every acquired reservation and is preserved on renewal/completion/retry.
All mutations check the identity, Processing state and expiration. Even an expired owner without
a successor is rejected. `bigint` exhaustion fails the acquisition explicitly; tokens never wrap or
reset. An exhausted oldest row requires operator investigation and can delay selection of later work;
this is a fail-closed exceptional condition, not an automatic token-recycling policy.

Lease durations are 1 second through 1 hour; retry delays are zero through 7 days. Durations must
have microsecond precision (multiples of 10 .NET ticks), matching PostgreSQL timestamps. These are
laboratory bounds, not a tuned production heartbeat/backoff policy. An expiration returned to the
caller is information, not permission to validate ownership using the worker's wall clock.

## Acquisition and authorization

Each operation opens a short READ COMMITTED transaction. Acquisition selects at most one due row
using `FOR UPDATE SKIP LOCKED`, ordered by eligibility time, receipt time and ID. Pending uses its
receipt time, RetryScheduled its next-attempt time, and Processing its expiration. Locked rows are
skipped, so NoEligibleWork means no eligible unlocked candidate was obtained, not an empty Inbox.
This is not strict FIFO, a fairness bound, or an elimination of table-lock waits.

After selection and row locking, a separate UPDATE obtains `clock_timestamp()` in a materialized
CTE and revalidates eligibility. It atomically increments generation and assigns the reservation.
Only successful commit confirmation exposes notification content and the lease. No business work,
network call or long computation belongs inside this transaction; connection and lock are disposed
before the operation returns.

Renewal/completion/retry first lock the target row. The following UPDATE obtains a fresh database
instant after any lock wait, validates state/owner/generation and `expiration > instant`, and mutates
under that same lock. Equality means expired. Authorization is at this validation point, not at the
later commit or response time. A transaction validated before expiration can finish committing
after expiration while retaining its lock; a successor cannot pass it in the meantime.

`now()` is the transaction start and `statement_timestamp()` is the statement start; neither is
appropriate for authorization after a lock wait. Statement time is only a discovery cutoff.
Renewal uses the greater of current expiration and database-now plus duration, without shortening
the lease. Retry eligibility is database-now plus delay. Database time removes worker clock skew,
but civil-clock adjustments can advance/delay expiry. Generations still fence prior acquisitions.

Transactions require a writable primary with fsync/full_page_writes enabled and force
`synchronous_commit=on`. Local limits are lock wait 2 seconds, statement 5 seconds, idle transaction
10 seconds, and Npgsql command 10 seconds. Connection establishment/pooling retain data-source
timeouts and caller cancellation. These limits bound ordinary waits; they are not a hard real-time
commit deadline or a guarantee against server/storage stalls. No transaction is held across calls.

## Failures, recovery and external effects

Before-commit failures propagate without granting a confirmed lease. Transient provider failures
are translated to Application's storage-unavailable exception. During the commit boundary, loss
of confirmation, timeout or cancellation produces OutcomeUnknown. The server may already have
committed; this must not be described as rollback. There are no hidden retries.

- An uncertain acquisition exposes no work. Do not start processing from it; let any committed
  reservation expire and become available again.
- An uncertain renewal returns no confirmed expiration. Do not use it as indefinite authorization;
  suspend new steps and obtain a confirmed valid renewal or abandon the lease.
- An uncertain completion/retry must not trigger repetition of external effects. Persisted state
  or expiration determines subsequent recovery. Repeating an old lease operation can return
  LeaseLost even if its first execution succeeded; this is not proof of rollback.

After process restart, a new execution acquires eligible persisted work. Expired Processing can be
acquired directly with a strictly greater generation. Memory is not recovery state. Cancellation
does not forcibly stop an external call: do not release a reservation while old processing can
still run. Any future checkpoint must validate the lease and write atomically in one transaction.

Inbox exclusivity does not provide exactly-once external effects. Worker A may send to a POS and
lose the response, then worker B can acquire after expiration and send again. Fencing prevents A
from overwriting the Inbox but cannot undo or deduplicate its external request. External idempotency
needs a stable operation key and destination cooperation; it is outside this increment.

Repeatedly locked rows can starve; recurring failures need bounded backoff and monitoring of due
work age in the future worker. No advanced scheduler or retry orchestration is introduced here.

## Migration and index

Migration `002_inbox_processing.sql` leaves 001 immutable, expands state constraints, adds lease
fields and coherent state/field constraints, and preserves all old records as Pending/generation 0.
Deduplication keys, payloads and normalized version are unchanged. Registration never updates
processing metadata, including on redelivery after Processing, RetryScheduled or Completed.

The old `(state, received_at_utc, inbox_id)` index is replaced by one partial expression index
matching the acquisition CASE expression and ordering. It excludes Completed and supports the due
cutoff across all eligible states. There are no speculative separate indexes on every timestamp.
The integration suite checks its ordered range plan with sequential scans disabled on a small
fixture; that demonstrates index applicability, not production planner choice or throughput.

Provisioning applies missing versions sequentially under the existing advisory lock in one
transaction. It is repeatable and remains explicit, never part of HTTP startup. Using the existing
local database setup in [inbox-postgresql.md](inbox-postgresql.md):

```powershell
dotnet run --project Ordering.Inbox.TestHost -c Release -- migrate
dotnet test Ordering.Infrastructure.PostgreSql.Tests -c Release
```

`INBOX_TEST_CONNECTION` must point at a real disposable PostgreSQL with CREATEDB privileges for
the test fixture. The existing CI job runs this same project against PostgreSQL 17.7, including
the lease tests; publication, attestation and activation conditions are untouched.

## Verification boundaries

Tests use independent connections, row locks, pg_stat_activity observation and internal fault
boundaries. Controlled SQL changes to expiration/due times avoid relying on arbitrary sleeps;
these are test setup operations, not public lease APIs. The single-shot acquire command in
Ordering.Inbox.TestHost is a process-restart test probe, not an executable background worker.

Coverage includes contention, skip-locked selection, fencing, expiration without a successor,
renewal/recovery races, time validation after a lock wait, retries, terminal completion, actual
commit/rollback with injected lost confirmation, cancellation/lock timeout, v1 migration with data,
repeatable provisioning, constraints, generation exhaustion, HTTP redelivery and released row locks.

These are local PostgreSQL guarantees. Injected confirmation loss tests classification, not physical
packet loss. Process exit tests recovery without shared memory, not server power loss. Disk flush
honesty, crash recovery under power loss, backup restore, replication, failover, network partitions,
production scale/fairness and external idempotency remain unverified. There is no production
activation, Uber order lookup, POS integration, Kafka or Transactional Outbox.

## Local verification — 2026-11-11

All six test projects were cleaned and rebuilt in Release for net8.0 using SDK 10.0.101
and the installed .NET 8 runtime. Every build reported zero errors and zero warnings.
The complete suite then passed against PostgreSQL 17.7 in an isolated local container:

- Domain: 7 passed.
- Contracts.Partners: 16 passed.
- Integrations.UberEats: 120 passed.
- Application: 14 passed.
- API: 38 passed.
- PostgreSQL/HTTP: 76 passed, including 40 processing-coordination cases and 3 new HTTP regressions.

Total: **271 passed, 0 failed, 0 skipped**. The initial test run exposed an incorrect expectation
that SKIP LOCKED must acquire immediately during a rejected renewal; the test now accepts a
temporary NoEligibleWork and retries after the competing transaction ends. Lock-observation tests
also received an explicit longer test-only lock window to synchronize before timeout. An initial
sandboxed HTTP test could not write the Windows event log; final verification ran outside that
sandbox. These issues were resolved before the complete clean verification reported above.

The source diff was reviewed and `git diff --check` passed. The CI workflow includes these tests
through its existing test-project command, but the remote workflow was not executed in this session.
Only the local, bounded guarantees described above were tested; production infrastructure and
exactly-once external effects were not verified. No implementation beyond the approved scope was added.
