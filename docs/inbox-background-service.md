# Worker contínuo da Inbox

## Composição e arquitetura

`Ordering.Hosting` é uma biblioteca `net8.0`. Ela referencia Application e
`Microsoft.Extensions.Hosting.Abstractions` 8.0.1 e contém o `InboxBackgroundService`.
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
liberar o escopo. A composição de teste resolve dentro do delegate já protegido pelo
`await using` do worker, garantindo o descarte também se a resolução lançar exceção.

Para integrar futuramente, construa `InboxWorkerExecution` com o método do coordenador
e o escopo dedicado, forneça a fábrica ao serviço e registre explicitamente o serviço
no host autorizado. A fábrica e `RunOnceAsync` devem devolver prontamente seus objetos
e Tasks. Uma instância do serviço corresponde a uma execução do worker.

A composição isolada em `InboxWorkerScopeTests` usa `IServiceScopeFactory.CreateAsyncScope`,
`ValidateScopes` e `ValidateOnBuild`. Os testes incluem a rejeição de uma dependência
Scoped capturada por um Singleton e de resolução Scoped no contêiner raiz. Hosting e
Npgsql adicionais pertencem exclusivamente ao projeto de testes.

A entrega fornece a biblioteca e testes com doubles. A ativação em Inbox persistente
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
depois desse descarte. Os testes mantêm a raiz viva até a quiescência e usam apenas
Stores e Processors sintéticos; não validam shutdown seguro com recursos reais.

Uma solução operacional mais ampla poderia coordenar o encerramento e o descarte do
contêiner raiz com a quiescência de todos os workers, definindo também o tratamento de
tarefas que nunca terminam e do prazo externo de término do processo. Isso requer
decisões de arquitetura e hospedagem e permanece fora deste incremento.

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

## Verificação local em 16/11/2026

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
