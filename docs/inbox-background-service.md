# Worker contínuo da Inbox

## Composição e arquitetura

`Ordering.Hosting` é uma biblioteca `net8.0`. Ela referencia Application e
`Microsoft.Extensions.Hosting` 8.0.1 e contém o `InboxBackgroundService`.
O vínculo com hospedagem fica nessa camada. O coordenador continua responsável pelas
operações de aquisição, renovação, finalização e retry de cada tentativa.

O construtor recebe uma fábrica `Func<InboxWorkerExecution>`, opções imutáveis,
`TimeProvider` e logger. A fábrica é chamada em cada tentativa, depois de verificar o
cancelamento do host. Cada execução de `ExecuteAsync` gera um owner GUID novo e usa
esse owner em todas as suas tentativas. Reiniciar o worker cria outra identidade.
A exclusividade tem a garantia probabilística de um GUID.

`InboxWorkerExecution` recebe o delegate `coordinator.RunOnceAsync` e um
`IAsyncDisposable` que possui o grafo de dependências Scoped de uma tentativa.
O delegate é a fronteira mínima para simulação nos testes. Cada tentativa cria um
escopo assíncrono novo, resolve nele o coordenador e suas dependências, executa o
coordenador, observa o resultado, aguarda integralmente `Quiescence` e descarta o
escopo. Só depois aplica polling ou backoff e considera a próxima aquisição.
Nenhum escopo permanece alocado durante a espera entre tentativas.

Os lifetimes da composição são: `InboxBackgroundService` Singleton;
`InboxProcessingCoordinator`, `IInboxProcessingStore` e `IInboxNotificationProcessor`
Scoped; `NpgsqlDataSource` Singleton pertencente ao contêiner raiz. O mesmo DataSource
é compartilhado pelos Stores dos diferentes escopos, sem criar um pool por tentativa.
O descarte de um escopo não deve descartar esse Singleton.

O responsável pela composição transfere a posse do escopo a `InboxWorkerExecution`.
Se resolver as dependências antes dessa transferência e a resolução falhar, deve
liberar o escopo. A composição de teste resolve dentro do delegate já protegido pela
fronteira de descarte assíncrono do worker, garantindo o descarte também se a resolução lançar exceção.

Para integrar futuramente, construa `InboxWorkerExecution` com o método do coordenador
e o escopo dedicado, forneça a fábrica ao serviço e registre explicitamente o serviço
no host autorizado. A fábrica e `RunOnceAsync` devem devolver prontamente seus objetos
e Tasks. Uma instância do serviço corresponde a uma execução do worker.

A composição isolada em `InboxWorkerScopeTests` usa `IServiceScopeFactory.CreateAsyncScope`,
`ValidateScopes` e `ValidateOnBuild`. Os testes incluem a rejeição de uma dependência
Scoped capturada por um Singleton e de resolução Scoped no contêiner raiz.
Npgsql pertence exclusivamente ao projeto de testes de Hosting.

A entrega fornece a biblioteca, testes com doubles e um cenário isolado de shutdown
com Store PostgreSQL real, descrito abaixo. A ativação em Inbox persistente
depende de um processador de negócio adequado e de composição explícita. Os hosts
existentes continuam oferecendo os fluxos já registrados. Os processadores sintéticos
dos testes ficam no projeto de testes.

## Políticas implementadas

- `NoEligibleWork` aguarda `PollingDelay`, inicialmente 5 segundos.
- `Completed`, `RetryScheduled` e `LeaseLost` permitem a próxima aquisição imediatamente
  depois da quiescência da tentativa anterior.
- `StoreFailed` aplica backoff exponencial individual de 2, 4, 8, 16 e 30 segundos,
  repetindo o limite em falhas consecutivas. `InitialStoreFailureDelay` e
  `MaximumStoreFailureDelay` configuram início e limite. Qualquer outro status reinicia
  a progressão.
- `ProcessingFailed` registra o erro e aguarda `ProcessingFailureDelay`, inicialmente
  5 segundos.
- `OutcomeUnknown` registra o resultado e aguarda `UnknownOutcomeDelay`, inicialmente
  5 segundos. A próxima chamada é uma nova tentativa de aquisição pelo coordenador.
- O cancelamento do host encerra o loop após a observação do trabalho pendente, qualquer
  que seja o status final da tentativa. `Cancelled` sem cancelamento do host é registrado
  e aguarda `ProcessingFailureDelay`, prevenindo um ciclo rápido inesperado.

Como proteção provisória, após `MaximumImmediateResults` resultados imediatos
consecutivos, inicialmente 100, o worker aguarda `ImmediateResultDelay`, inicialmente
100 ms. A contagem abrange os três status imediatos, inclusive sequências mistas.
Uma tentativa cuja duração alcance esse intervalo, uma espera normal ou a própria
pausa de proteção reinicia a contagem. Um `LeaseLost` isolado conserva o tratamento
imediato. A duração é medida pelo relógio monotônico do `TimeProvider`.

Todas as esperas usam `Task.Delay` com o `TimeProvider` recebido e token do host.
As opções são validadas antes de criar recursos. Intervalos devem ser de pelo menos
1 ms e respeitar o limite dos timers do .NET. O limite de backoff deve ser maior ou
igual ao início e o limite de resultados imediatos deve ser pelo menos 2.
Jitter permanece como otimização futura.

## Quiescência e shutdown

O worker examina `ShutdownTimedOut` e registra seu esgotamento. A task `Quiescence`
é aguardada integralmente, mesmo com `ShutdownTimedOut`, antes do descarte do escopo
e de qualquer aquisição seguinte. Uma quiescência que falha é observada e registrada
como crítica; seu término permite liberar o escopo e encerra novas aquisições.

`StopAsync` transmite o cancelamento cooperativo pelo mecanismo de `BackgroundService`.
O token recebido de quem chama `StopAsync` representa o prazo de shutdown do host.
Seu vencimento limita essa espera e produz um registro crítico quando `ExecuteTask`
ainda está pendente. O orçamento de shutdown do coordenador continua independente.

Mesmo depois de `StopAsync` retornar e `Dispose` ser chamado, `ExecuteTask` mantém a
supervisão da chamada em andamento e da quiescência devolvida por ela. O descarte
assíncrono do escopo ocorre depois dessa observação. Enquanto houver tarefas pendentes,
o worker mantém o escopo e suas referências. A ausência permanente de quiescência
conserva essa retenção até o término do processo. O host ou responsável
pelo lifetime pode acompanhar a supervisão pela propriedade pública `ExecuteTask`.

**Limite do contêiner raiz:** reter um escopo não impede que o host descarte o contêiner
raiz depois de esgotar o prazo de shutdown. O `NpgsqlDataSource` Singleton poderá ser
descartado mesmo com `Quiescence` pendente. Não há garantia de que continue válido
depois desse descarte. Os testes mantêm a raiz viva até a quiescência. O cenário
PostgreSQL abaixo exercita um Store real com Processor sintético; não valida uso
seguro de recursos após descarte da raiz.

A composição isolada descrita abaixo coordena a quiescência dos workers registrados
e fornece autorização explícita para descarte normal da raiz. O término definitivo
de operações que nunca concluem exige política operacional e supervisor externo.

Esse comportamento limita o tempo aguardado pelo host, sob as condições usuais de
agendamento do runtime e implementações assíncronas que devolvem Tasks prontamente.
A supervisão só persiste enquanto o processo estiver vivo. Cancelar cooperativamente
ou observar o término local de uma Task deixa os efeitos HTTP remotos sujeitos ao
comportamento do sistema remoto. Exactly-once e interrupção de operações remotas
permanecem sem garantia.

Exceções que escapam de `RunOnceAsync` ficam em `ExecuteTask` e seguem a supervisão de
falhas do host, sem retries automáticos. Se uma implementação defeituosa iniciar tarefas
desanexadas e lançar exceção sem fornecer resultado com `Quiescence`, o worker não tem
como descobri-las ou aguardá-las; o escopo será liberado ao sair da chamada. A retenção
até quiescência pressupõe o cumprimento desse contrato. Os resultados esperados do
coordenador são tratados pelas políticas acima. O comportamento de falha do host depende
de sua configuração.

## Coordenação global no Generic Host isolado

`InboxShutdownCoordinator` recebe um `IHost`, o orçamento global de drenagem e um
`TimeProvider`. Os participantes são as instâncias de `InboxBackgroundService` registradas
como `IHostedService`, na ordem de registro. Registros vazios ou duplicados são rejeitados.
O orçamento é obrigatório, finito e de pelo menos 1 ms, dentro do limite dos timers .NET.

A composição deve configurar as opções antes de construir o host.

```csharp
services.Configure<HostOptions>(options =>
{
    options.ServicesStopConcurrently = true;
    options.ShutdownTimeout = Timeout.InfiniteTimeSpan;
});
```

No .NET 8, `ServicesStopConcurrently` permite ao host chamar os `StopAsync` seguintes
assim que a parte síncrona de cada chamada devolve sua Task. Isso transmite a parada
a todos os participantes antes da espera integral pela drenagem. O timeout interno
infinito deixa o orçamento sob controle do relógio fornecido ao coordenador.
As implementações precisam devolver Tasks prontamente, inclusive callbacks de cancelamento.
O comportamento foi conferido no [código do Generic Host 8.0.1](https://github.com/dotnet/runtime/blob/v8.0.1/src/libraries/Microsoft.Extensions.Hosting/src/Internal/Host.cs).

Depois de `await host.StartAsync()`, o proprietário explícito do ciclo de vida pode executar
o trecho seguinte, com orçamento e relógio fornecidos pela composição.

```csharp
var shutdown = new InboxShutdownCoordinator(host, configuredDrainTimeout, timeProvider);
var result = await shutdown.ShutdownAsync();
```

Esse coordenador deve ser o único responsável pela chamada a `Host.StopAsync`.
Chamadas repetidas e concorrentes a `ShutdownAsync` compartilham a mesma Task e o mesmo
resultado. O contrato de propriedade exige conduzir o ciclo de vida explicitamente,
incluindo retenção e descarte do host. `RunAsync`, `WaitForShutdownAsync` e chamadas
independentes a `StopAsync` assumem responsabilidades de parada ou descarte que conflitam
com esse proprietário. A composição de aceitação está em `InboxShutdownCoordinatorTests`.

O resultado contém o estado de parada do host, uma indicação de falha dessa parada e
snapshots dos participantes, identificados pela posição na lista de registros.
Cada snapshot distingue solicitação de parada, término comprovado das operações,
conclusão de `ExecuteTask`, descarte dos escopos e falhas de supervisão.
Os diagnósticos utilizam categorias fixas, sem conteúdo de eventos, conexão ou exceções.

- `Drained` exige parada do host concluída, todos os workers sinalizados, suas operações
  supervisionadas concluídas, `ExecuteTask` concluídas e escopos descartados com sucesso.
  `RootDisposalAuthorized` permite então o descarte normal da raiz pelo proprietário.
  A garantia abrange os participantes registrados e o trabalho declarado pelo contrato
  de supervisão. Consumidores externos exigem sua própria coordenação.
- `DeadlineExceeded` indica orçamento global esgotado enquanto a conclusão ainda era
  aguardada. O mesmo timer abrange operações, quiescência e descarte assíncrono dos escopos.
  A medição monotônica também detecta conclusão observada depois do orçamento.
- `Unverified` indica supervisão comprometida. Essa classificação tem precedência quando
  também há vencimento do prazo. A evidência fica gravada antes de aguardar o descarte,
  permitindo observar a falha mesmo com `ExecuteTask` pendente ou concluída normalmente.

`InboxBackgroundService.ShutdownState` expõe a evidência corrente. Falhas capturadas ao
aguardar o resultado/quiescência ficam registradas como `ResultSupervision`. Exceções
inesperadas da execução continuam propagadas sem retry e recebem a categoria `Execution`.
Falhas de descarte recebem `ScopeDisposal`. Essas classificações expressam ausência de
comprovação suficiente. Uma Task faulted, por si só, deixa a existência de trabalho órfão
indeterminada.

Quando um delegate ou a composição conhece uma operação destacada, deve chamar
`worker.ReportUntrackedWork()` antes de concluir a execução supervisionada. O marcador
`KnownUntrackedWork` é permanente. A descoberta genérica dessas operações permanece
fora das capacidades do runtime.

Em `DeadlineExceeded` e `Unverified`, o proprietário deve manter o host e a raiz vivos.
O coordenador mantém a chamada original a `Host.StopAsync` e a observação de todas as
`ExecuteTask`. `shutdown.Completion` permite acompanhar esse término posterior,
inclusive suas falhas observadas. O snapshot original conserva sua classificação e sua
autorização. O término posterior exige avaliação explícita das evidências para liberar recursos.

O timeout limita a espera do coordenador. Operações PostgreSQL dependem de seu próprio
cancelamento e término efetivo. O `NpgsqlDataSource` permanece Singleton de propriedade
da raiz, cujo descarte é responsabilidade do proprietário do host. A aplicação real
necessitará de supervisor externo e política operacional explícita para o término
definitivo em caso de bloqueio ou supervisão comprometida.

As fixtures retêm a raiz, liberam as operações sintéticas e aguardam a chamada original
de parada antes de descartar explicitamente o host. Esse controle inclui trabalho destacado
conhecido e descartes suspensos. O incremento permanece restrito à composição isolada.

### Shutdown com operação PostgreSQL real em 17/11/2026

`PostgresInboxShutdownTests` reutiliza `DatabaseFixture`, com banco isolado, e o
`ControlledTimeProvider` compartilhado pelos testes de Application e Hosting.
Um Generic Host validado possui um único worker, coordenador e Store por escopo,
Processor sintético e seu próprio `NpgsqlDataSource` Singleton criado pela raiz.
A fixture mantém um pool independente para bloqueio e observação.

Após aquisição confirmada e início do Processor, uma transação auxiliar bloqueia
a linha com `SELECT ... FOR UPDATE`. O timer controlado inicia `RenewAsync` real.
O hook interno `BeforeLock` identifica o PID e ajusta apenas nessa composição os
limites locais de lock/statement para 8/9 segundos. A observação exige sessão ativa,
`wait_event_type = 'Lock'`, consulta `FOR UPDATE` e PID da transação auxiliar em
`pg_blocking_pids`. Uma Task pendente isoladamente não satisfaz essa verificação.

O shutdown é solicitado por `InboxShutdownCoordinator`. O Processor recebe
cancelamento e permanece retido por sinal. O teste dispara os prazos local e global,
verifica `ShutdownTimedOut`, `Quiescence` pendente e snapshot `DeadlineExceeded`,
sem autorização de descarte da raiz. O escopo permanece vivo, a raiz ainda executa
SQL real e não há nova aquisição. Cancelar Npgsql pode encerrar a renovação antes
da liberação manual do lock; não se exige espera de lock após o cancelamento.

Depois de liberar a transação e o Processor, o teste aguarda `Completion` e
`Quiescence`, confere descarte dos recursos Scoped, conclusão do worker e ausência
de falhas de supervisão. O snapshot original permanece `DeadlineExceeded`, mesmo
com conclusão posterior. O estado persistido continua `Processing`, com geração 1
e owner preenchido; não há chamada de conclusão/retry após cancelamento. Isso não
afirma rollback de uma renovação nem preservação do valor anterior de expiração.
O descarte explícito final invalida apenas o DataSource da raiz; a fixture ainda
executa SQL. Bloqueio e sinais sintéticos são liberados em `finally`.

O cenário cobre um worker e uma renovação bloqueada, com limites .NET controlados
e guardas reais contra deadlock. POS, Outbox, Kafka, ativação operacional, supervisor
externo e segurança de operações após descarte prematuro da raiz permanecem fora
desta evidência. As políticas de produção e de publicação não foram alteradas.

Verificação local em Release, target/runtime .NET 8 e SDK 10.0.101: PostgreSQL 17.7,
77 testes aprovados; Hosting, 51; Application, 71. Total: 199, sem falhas ou skips
nas execuções finais. Comandos: `dotnet test <projeto>/<projeto>.csproj -c Release
--no-restore`, com `INBOX_TEST_CONNECTION` apontando para PostgreSQL descartável.
Na primeira execução completa, um teste HTTP existente falhou por acesso bloqueado
ao Event Log do Windows; a suíte PostgreSQL inteira passou na reexecução autorizada
fora do sandbox. O teste novo também passou isoladamente. `git diff --check` passou.

### Verificação da coordenação global em 16/11/2026

Base conferida antes das alterações em `teste-pr`, HEAD
`b0829ec1e418803207099f4a629e7e3f4a9876f4`, árvore limpa.
Compilação local com SDK 10.0.101, target `net8.0` e execução em runtime .NET 8.

- Hosting, 51 testes aprovados, incluindo 12 casos novos de coordenação global.
- Application, 71 aprovados, incluindo regressões de leases e shutdown.
- API, 38 aprovados.
- Domain, 7 aprovados.
- Contracts.Partners, 16 aprovados.
- Integrations.UberEats, 120 aprovados.

Total local de 303 aprovados, zero falhas e zero skips nas execuções finais.
Os comandos utilizaram `dotnet test <projeto>/<projeto>.csproj -c Release`, com
`--no-restore` após o restore inicial. O restore precisou de execução autorizada para
leitura de `NuGet.Config`. A suíte API passou após reexecução autorizada para permitir
o logger do Event Log do Windows, que havia sido bloqueado no sandbox.

Os novos casos utilizam `TaskCompletionSource`, sinais de cancelamento e timers de
disparo explícito. Limites de tempo real de 10 segundos funcionam como guardas contra
deadlock. Os testes cobrem três workers em conclusões distintas, sinalização concorrente,
idempotência da parada, retenção durante quiescência e descarte assíncrono, prazo único,
falha capturada de quiescência, execução inesperada sem retry, trabalho destacado conhecido,
falhas de descarte e parada do host, evidência de falha com descarte pendente e validação
de composição e orçamento. A ausência de novas aquisições é conferida após a parada.

O DataSource real é criado como Singleton da raiz e compartilhado pelos escopos.
As verificações exercitam a composição e objetos de conexão fechados. Após descarte
explícito do host, a tentativa de abrir conexão falha por descarte do DataSource antes
de alcançar a rede. As operações de drenagem são doubles e nenhuma conexão é aberta.
`INBOX_TEST_CONNECTION` estava ausente, portanto a suíte PostgreSQL permanece pendente
localmente. A coordenação com PostgreSQL e POS reais e o comportamento do supervisor
externo ainda exigem validação operacional.

## Verificação do incremento anterior em 16/11/2026

Base conferida antes das alterações: `teste-pr`, HEAD
`0c90192c76128fa6834a2b905dccd7616314ce44`, árvore de trabalho limpa.
Compilação Release com SDK 10.0.101, target e execução dos testes em .NET 8.

```powershell
dotnet test Ordering.Hosting.Tests/Ordering.Hosting.Tests.csproj -c Release
dotnet test Ordering.Application.Tests/Ordering.Application.Tests.csproj -c Release --no-restore
dotnet test Ordering.API.Tests/Ordering.API.Tests.csproj -c Release --no-restore
dotnet test Ordering.Dominio.Tests/Ordering.Dominio.Tests.csproj -c Release --no-restore
dotnet test Ordering.Contracts.Partners.Tests/Ordering.Contracts.Partners.Tests.csproj -c Release --no-restore
dotnet test Ordering.Integrations.UberEats.Tests/Ordering.Integrations.UberEats.Tests.csproj -c Release --no-restore
git diff --check
```

Resultados finais das suítes executadas: 291 testes aprovados, sem falhas ou skips.

- Hosting: 39 aprovados (30 casos do worker e 9 casos com Generic Host isolado).
- Application: 71 aprovados.
- API: 38 aprovados.
- Domain: 7 aprovados.
- Contracts: 16 aprovados.
- Integrations.UberEats: 120 aprovados.

### Comportamentos efetivamente testados

Os testes de Hosting usam tarefas sinalizadas e temporizadores de disparo explícito.
O relógio monotônico do helper compartilhado é controlado pelo teste. Limites reais
de 10 segundos servem apenas como guardas contra deadlock; não há `Thread.Sleep`.

Os casos do worker verificam polling configurável, backoff individual e reset por cada
status, esperas de falha, owners, proteção contra hot loop, cancelamento, supervisão
após os prazos e exceções inesperadas síncronas/assíncronas sem retries. A fixture cria
recursos diferentes por tentativa e verifica que os anteriores foram descartados
antes de cada aquisição, também nos caminhos imediatos e de backoff.

A composição com Generic Host verifica:

- Worker Singleton, coordenador/Store/Processor Scoped e validação efetiva do contêiner
  com `ValidateScopes` e `ValidateOnBuild`, incluindo casos negativos.
- Coordenadores, Stores e Processors diferentes entre tentativas, com a mesma instância
  Singleton de `NpgsqlDataSource`, criada pela raiz. Nenhuma conexão é aberta; criar um
  objeto de conexão fechado confirma que o descarte do escopo não descartou o DataSource.
- Descarte do escopo antes do polling e da aquisição seguinte; o worker aguarda também
  o término de `DisposeAsync`, sem criar novos escopos durante esse intervalo.
- Retenção de recursos Scoped durante quiescência sinalizada, inclusive quando o resultado
  indica `ShutdownTimedOut`; falha tardia de quiescência registrada como crítica e sem
  novas aquisições. Essa falha é injetada por decoração explícita do resultado nos testes.
- Coordenador real com Store sintético que ignora cancelamento, atravessando os prazos
  do host e do coordenador, com descarte só após sucesso ou falha tardia da aquisição.
- Cancelamento durante quiescência e descarte assíncrono sem novos escopos ou polling;
  início e término dos hosts e owner estável entre tentativas, diferente após reinício.

O restore exigiu execução autorizada fora do sandbox para ler `NuGet.Config`. Na suíte
API, um caso falhou inicialmente por bloqueio do log de eventos do Windows; a suíte
inteira passou na reexecução autorizada.

### Limitações e verificações pendentes

A suíte `Ordering.Infrastructure.PostgreSql.Tests` não foi executada, pois
`INBOX_TEST_CONNECTION` não estava configurada. Os testes deste incremento não executam
persistência, POS, chamadas remotas ou processamento de negócio reais. Não há validação
ponta a ponta com PostgreSQL e POS reais, nem de shutdown seguro após descarte da raiz.

As mudanças permanecem locais, sem novo commit ou push; portanto não há execução de CI
correspondente a este incremento disponível para verificar. O workflow existente já
executa Hosting no job `test` e não foi alterado. Docker, publicação, deploy, migrações
e os registros dos hosts HTTP permanecem sem alterações.
