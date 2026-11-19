# Worker contínuo da Transactional Outbox — incremento 4

## Contrato e responsabilidades

`Ordering.Hosting` contém `OutboxBackgroundService`, sem dependência de PostgreSQL ou
Kafka. O serviço Singleton repete sequencialmente `OutboxRelay.RunOnceAsync`. O Relay
permanece responsável por uma aquisição, validação de `OrderConfirmed`, envio com ACK,
conclusão ou retry; Store e produtor conservam suas responsabilidades e contratos.
Nenhum serviço foi registrado em `Ordering.API` ou `Ordering.API.LaboratoryHost`.

O construtor recebe uma fábrica `Func<ValueTask<OutboxWorkerExecution>>`, opções
próprias, `TimeProvider` e logger. Cada execução gera um owner GUID e inicia a primeira
tentativa automaticamente. `OutboxWorkerExecution.CreateScopedAsync` cria um escopo
assíncrono por tentativa, resolve `OutboxRelay` Scoped e transfere a posse do escopo
ao worker. Se a resolução falhar, descarta o escopo antes de propagar a falha.

O worker aguarda integralmente a Task do Relay, descarta o escopo e só então aplica
a política de pausa ou permite nova aquisição. Nenhum escopo fica alocado no polling.
`IOutboxProcessingStore` é Scoped; `NpgsqlDataSource` e `KafkaOutboxEventPublisher`
são Singletons criados e possuídos pela raiz. A mesma sessão de produção e o mesmo
pool são reutilizados entre tentativas; o worker não os descarta.

## Política efetiva — valores provisórios de laboratório

Todos os valores abaixo são configuráveis em `OutboxWorkerOptions`. Não são
parâmetros de produção aprovados.

- `Published` e `LeaseLost`: permitem próxima tentativa sem pausa obrigatória.
- `NoEligibleWork`: `PollingDelay`, inicialmente 5 segundos.
- `RetryScheduled`: `PublicationFailureDelay`, inicialmente 5 segundos.
- `AcquisitionUnknown`, `CompletionUnknown` e `RetryUnknown`: `UnknownOutcomeDelay`,
  inicialmente 5 segundos. O worker não repete a conclusão anterior; só inicia uma
  nova tentativa de aquisição, respeitando elegibilidade, leases e fencing do Store.
- `OperationalFailure`: backoff exponencial de 2, 4, 8, 16 e 30 segundos, mantendo
  o teto, por `InitialOperationalFailureDelay` e `MaximumOperationalFailureDelay`.
- `Cancelled`: encerra se o host está parando; caso contrário aguarda
  `PublicationFailureDelay`, também evitando busy polling após timeout do Relay.
- `UnsupportedEvent`: registra `Critical`, EventId, owner e estágio, encerra novas
  aquisições e exige intervenção. O host pode continuar vivo; isso não indica liveness
  do worker. É necessário corrigir o evento/configuração e reiniciar explicitamente.

O backoff só reinicia após `Published` (progresso confirmado) ou `NoEligibleWork`
(consulta de elegibilidade confirmada). Retry, resultados incertos, perda de lease
e cancelamento preservam o valor anterior. Essa regra mede recuperação do ciclo do
Relay, e uma consulta ociosa bem-sucedida não prova disponibilidade do Kafka.

Resultados imediatos consecutivos, inclusive sequências mistas de `Published` e
`LeaseLost`, são limitados por `MaximumImmediateResults` (100). Ao atingir o limite,
aguarda `ImmediateResultDelay` (100 ms) e reinicia a contagem. A contagem independe
da duração da tentativa; qualquer resultado que exige pausa também a reinicia.
Todas as esperas usam `Task.Delay` com `TimeProvider` e token cancelável do host.
Intervalos devem ser de pelo menos 1 ms e respeitar o limite de timer .NET
(`uint.MaxValue - 1` milissegundos). O teto deve ser pelo menos o início do backoff
e o limite de resultados imediatos deve ser positivo e finito.

## Supervisão, quiescência e graceful shutdown

O contrato do adaptador Kafka existente mantém o envio iniciado observado até seu
delivery report ou timeout, mesmo após cancelamento. O worker não usa fire-and-forget
nem `WaitAsync(stoppingToken)` para abandonar a Task do Relay. Cancelar não desfaz
uma mensagem Kafka; ACK tardio após cancelamento pode deixar o evento em `Processing`
para recuperação por lease. A publicação continua **at-least-once**.

A Outbox não oferece `Quiescence` separada. A conclusão de `RunOnceAsync` representa
o término das operações iniciadas e supervisionadas por seus adaptadores. O worker
depende desse contrato e não garante supervisão de tarefas externas desconhecidas.
Exceções inesperadas da fábrica, execução, logger ou descarte interrompem o loop e
mantêm evidência persistente na memória para um shutdown `Unverified`.

`OutboxShutdownCoordinator` é o único proprietário de `Host.StopAsync`. A composição
deve usar `StartAsync`, `ServicesStopConcurrently = true` e `ShutdownTimeout` infinito,
sem `Run`, `RunAsync`, `WaitForShutdownAsync` ou outro proprietário de parada/descarte.
O coordenador fornece o orçamento global, incluindo término do Relay, descarte de
escopos e stop do host. Chamadas repetidas compartilham a mesma Task e snapshot.
Fábricas, callbacks de cancelamento e métodos que devolvem Tasks precisam retornar
prontamente; um bloqueio síncrono não cooperativo impede observar o prazo em tempo.

- `Drained`: host parado, Tasks terminadas, operações concluídas, escopos descartados,
  sem falhas de supervisão. Somente esse resultado autoriza descarte normal da raiz.
- `DeadlineExceeded`: prazo esgotado sem evidência de falha, mantendo a supervisão
  em andamento. Reter host, produtor e raiz. `Completion` permite observar o término
  posterior. O snapshot histórico nunca se transforma automaticamente em `Drained`.
- `Unverified`: falha de execução, descarte ou stop do host, ou evidência insuficiente.
  Não autoriza descarte normal. Falhas conhecidas têm precedência sobre deadline.

`Completion` observa falhas tardias e aguarda todas as ExecuteTasks e a única parada
do host. Seu término sozinho não concede autorização de descarte. O worker não pode
impedir que um chamador descarte a raiz indevidamente; reter referências de escopo
não protege Singletons já descartados. Hard stop externo permanece fora do incremento.

## Iniciar uma instância isolada

A composição executável de referência está em `WorkerHost`, no arquivo de testes
`Ordering.Infrastructure.PostgreSql.Tests/KafkaOutboxWorkerTests.cs`. O teste inicia
o Generic Host e publica automaticamente, sem invocar `RunOnceAsync` manualmente:

```powershell
# PostgreSQL real com usuário que possa criar bancos isolados; não usar banco operacional.
$env:INBOX_TEST_CONNECTION = 'Host=localhost;Port=55432;Database=inbox_dev;Username=inbox_dev;Password=<senha-local>;Enlist=false'
dotnet test Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj -c Release --filter FullyQualifiedName~OutboxWorker
```

Docker Engine Linux deve estar disponível. A fixture existente cria um banco isolado,
aplica as migrações 001–004 e o remove ao terminar. Kafka real é criado/removido por
Testcontainers (`confluentinc/cp-kafka:7.6.1`); tópico e consumidor são exclusivos do
teste. O teste não ignora ausência de PostgreSQL/Docker.

Para uma instância interativa em um projeto console .NET 8 separado de laboratório,
referencie `Ordering.Hosting`, `Ordering.Infrastructure.PostgreSql` e
`Ordering.Infrastructure.Kafka` e use a composição abaixo. Provisione o schema com
as migrações existentes e um tópico Kafka previamente criado. As variáveis de
ambiente são específicas desse exemplo, sem configuração automática da API:

```csharp
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Ordering.Application.Outbox;
using Ordering.Hosting;
using Ordering.Infrastructure.Kafka;
using Ordering.Infrastructure.PostgreSql;

var host = new HostBuilder()
    .UseDefaultServiceProvider(o => { o.ValidateScopes = true; o.ValidateOnBuild = true; })
    .ConfigureLogging(l => l.AddConsole())
    .ConfigureServices(s =>
    {
        s.Configure<HostOptions>(o =>
        {
            o.ServicesStopConcurrently = true;
            o.ShutdownTimeout = Timeout.InfiniteTimeSpan;
        });
        s.AddSingleton<IHostLifetime, LaboratoryLifetime>();
        s.AddSingleton(_ => NpgsqlDataSource.Create(Required("OUTBOX_LAB_CONNECTION")));
        s.AddSingleton(_ => new KafkaOutboxEventPublisher(new()
        {
            BootstrapServers = Required("OUTBOX_LAB_BOOTSTRAP_SERVERS"),
            Topic = Required("OUTBOX_LAB_TOPIC")
        }));
        s.AddSingleton<IOutboxEventPublisher>(p => p.GetRequiredService<KafkaOutboxEventPublisher>());
        s.AddSingleton(new OutboxRelayOptions());
        s.AddScoped<IOutboxProcessingStore, PostgresOutboxProcessingStore>();
        s.AddScoped<OutboxRelay>();
        s.AddSingleton<IHostedService>(p => new OutboxBackgroundService(
            () => OutboxWorkerExecution.CreateScopedAsync(p.GetRequiredService<IServiceScopeFactory>()),
            new(), TimeProvider.System, p.GetRequiredService<ILogger<OutboxBackgroundService>>()));
    }).Build();

var coordinator = new OutboxShutdownCoordinator(host, TimeSpan.FromSeconds(45), TimeProvider.System);
await host.StartAsync();
Console.WriteLine("Worker ativo. Pressione Enter para solicitar parada supervisionada.");
Console.ReadLine();
var result = await coordinator.ShutdownAsync();
Console.WriteLine(result.Status);
if (result.RootDisposalAuthorized)
    await ((IAsyncDisposable)host).DisposeAsync();
else
{
    await coordinator.Completion; // Observação tardia; não muda o resultado histórico.
    Console.WriteLine("Raiz retida; intervenção necessária. Descarte normal não autorizado.");
    await Task.Delay(Timeout.InfiniteTimeSpan);
}

static string Required(string name) => Environment.GetEnvironmentVariable(name)
    ?? throw new InvalidOperationException($"{name} required.");

sealed class LaboratoryLifetime : IHostLifetime
{
    public Task WaitForStartAsync(CancellationToken ct) => Task.CompletedTask;
    public Task StopAsync(CancellationToken ct) => Task.CompletedTask;
}
```

O exemplo usa uma lifetime passiva e entrada explícita para manter um único dono da
parada. Ele não é um processo de produção nem uma implantação. Encerramento abrupto
do terminal/processo não comprova drenagem.

## Evidências e arquivos

- `Ordering.Hosting/OutboxBackgroundService.cs`: repetição, políticas, logs e estado
  de supervisão. `OutboxWorkerOptions.cs`: limites provisórios e validação.
- `Ordering.Hosting/OutboxWorkerExecution.cs`: contrato de tentativa e posse de escopo.
- `Ordering.Hosting/OutboxShutdownCoordinator.cs` e `OutboxShutdownResult.cs`: parada
  única, orçamento global, observação posterior e autorização conservadora de descarte.
- `Ordering.Hosting.Tests/OutboxBackgroundServiceTests.cs`: relógio controlado,
  polling, sequências imediatas, status incertos, backoff e interrupção.
- `Ordering.Hosting.Tests/OutboxWorkerScopeTests.cs` e `.csproj`: DI validada,
  Relay/Store distintos, mesmo produtor Kafka/DataSource, falha de resolução.
- `Ordering.Hosting.Tests/OutboxShutdownCoordinatorTests.cs`: drenagem, retenção da
  raiz após deadline, resultado imutável, falhas tardias, descarte e host.
- `Ordering.Infrastructure.PostgreSql.Tests/KafkaOutboxWorkerTests.cs`: três testes
  reais. `KafkaOutboxRelayTests.cs` passa a ser parcial para reutilizar suas fixtures
  e verificações; os testes existentes de duplicação, fencing e incerteza são preservados.
- `README.md` e este documento: uso e limites do incremento.

O caminho feliz verifica dois EventIds, pedidos distintos, payload exato, headers,
delivery report `Acknowledged`, tópico/partição/offset e `Published` no PostgreSQL.
A indisponibilidade recuperável eleva `min.insync.replicas` para 2 em broker com uma
réplica: o envio real não recebe ACK, persiste `RetryScheduled` e aguarda 7 segundos
controlados. Após restaurar ISR=1, a próxima tentativa automática publica e conclui.
O teste usa `RetryDelay = 0` apenas para isolar a pausa do worker da elegibilidade SQL.

A prova de shutdown inicia envio real e retém sua conclusão em uma **barreira
sintética de publicação**. Cancela, esgota o prazo, verifica raiz/escopo retidos,
libera a barreira e observa o término. Isso não equivale a interromper fisicamente
o broker. A limpeza desses testes só descarta após liberar e conhecer todas as
operações sintéticas; não converte `Completion` em autorização genérica.

Migrações, contratos Inbox, Relay e adaptador Kafka permanecem intactos. Permanecem
fora do escopo exactly-once, ordenação multi-evento, renovação automática de lease,
consumer de negócio, POS, Kubernetes, failover físico e publicação de imagens.

## Verificação local — 19/11/2026

Baseline conferida: `b4adf197c038c1f88bb2a1ebe2d3beb9ab205149`, branch `main`, árvore
inicial limpa. SDK 10.0.101, target .NET 8/runtime 8.0.26, PostgreSQL 17.7 e Kafka
7.6.1 reais em contêineres Linux no Docker Desktop. PostgreSQL temporário restrito
ao loopback, bancos isolados criados/removidos pelas fixtures, broker por Testcontainers.

Os **26 testes novos passaram**: 23 em Hosting e 3 com PostgreSQL/Kafka reais.
O teste específico do worker e a regressão executaram os cenários reais com sucesso.
Regressão completa, sem ignorar testes:

- Domain: 7 aprovados; Application: 95; Hosting: 74.
- Contracts: 16; Integração Uber Eats: 120; API: 38.
- PostgreSQL/Kafka: 176 aprovados e 1 falha.
- Total: **526 aprovados, 1 falha, zero ignorados**.

A falha é `CancellationDuringProvenLockWait_ReleasesTransactionWithoutMutation`,
teste existente em `PostgresOutboxProcessingTests.cs`. Na regressão, o lock timeout
do Store precedeu o cancelamento esperado e produziu `OutboxStorageUnavailableException`.
Isoladamente, a espera pela evidência de bloqueio terminou por cancelamento.
O mesmo teste também falhou por cancelamento da espera em uma cópia limpa da
baseline exata, exportada com `git archive`, compilada e executada separadamente.
Portanto, a regressão não está inteiramente verde neste ambiente; essa falha foi
reproduzida antes do incremento. O teste e a implementação do Store foram preservados.

Comandos de regressão (com `INBOX_TEST_CONNECTION` previamente configurada):

```powershell
$projects = @('Ordering.Dominio.Tests', 'Ordering.Application.Tests', 'Ordering.Hosting.Tests', 'Ordering.Contracts.Partners.Tests', 'Ordering.Integrations.UberEats.Tests', 'Ordering.API.Tests', 'Ordering.Infrastructure.PostgreSql.Tests')
foreach ($project in $projects) {
    dotnet test "$project/$project.csproj" -c Release --logger 'console;verbosity=minimal' --logger "trx;LogFileName=$project.trx" --results-directory "$env:TEMP\ordering-outbox-increment4-tests"
}
git diff --check
docker run --rm --volume 'C:\EstudosDevOps:/repo:ro' --workdir /repo rhysd/actionlint:1.7.12 -color
```

`git diff --check` e `actionlint`: aprovados. Relatórios TRX em
`%TEMP%\ordering-outbox-increment4-tests`, incluindo `outbox-worker.trx`, os sete
projetos e `baseline-cancellation.trx`. As primeiras tentativas de execução exigiram
permissão de acesso a Docker/NuGet; os resultados acima são das execuções autorizadas
com infraestrutura provisionada.
O exemplo console da documentação foi extraído e compilado separadamente: zero
avisos e zero erros. Sua execução interativa não foi realizada; a composição e o
fluxo real de publicação foram exercitados pelos testes de Generic Host.

O workflow existente já executa os projetos Hosting e PostgreSQL/Kafka, incluindo
automaticamente os novos testes. A CI remota deste incremento não foi executada;
os resultados locais não comprovam seu sucesso. Não houve push nem alteração no
workflow ou em `ENABLE_IMAGE_PUBLISH`. O SHA do commit local é informado na entrega.
Interrupção física do broker, hard stop externo e comportamento após descarte indevido
da raiz continuam não verificados.
