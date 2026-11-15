# Worker contínuo da Inbox

## Composição e arquitetura

`Ordering.Hosting` é uma biblioteca `net8.0`. Ela referencia Application e
`Microsoft.Extensions.Hosting.Abstractions` 8.0.1 e contém o `InboxBackgroundService`.
O vínculo com hospedagem fica nessa camada. O coordenador continua responsável pelas
operações de aquisição, renovação, finalização e retry de cada tentativa.

O construtor recebe uma fábrica `Func<InboxWorkerExecution>`, opções imutáveis,
`TimeProvider` e logger. A fábrica é chamada no início de `ExecuteAsync`. Cada execução
gera um owner GUID novo e usa esse owner em todas as suas tentativas. Reiniciar o
worker cria outra identidade. A exclusividade tem a garantia probabilística de um GUID.

`InboxWorkerExecution` recebe o delegate `coordinator.RunOnceAsync` e um
`IAsyncDisposable` que possui o grafo de dependências utilizado pelas tentativas.
O delegate é a fronteira mínima para simulação nos testes. A fábrica permite alocar
recursos somente quando o serviço começa.

O responsável pela composição deve criar recursos dedicados e transferir sua posse
à execução. O objeto descartável deve possuir todos os recursos compartilhados entre
as tarefas do coordenador, store e processador, incluindo conexões e clientes que
necessitem de descarte. Um escopo DI só atende a essa condição se todos esses recursos
forem de propriedade do escopo. Singletons de propriedade do contêiner raiz exigem
outra composição de lifetime, pois o host pode descartá-los ao atingir seu prazo.

Para integrar futuramente, construa `InboxWorkerExecution` com o método do coordenador
e o lifetime dedicado, forneça a fábrica ao serviço e registre explicitamente o serviço
no host autorizado. A fábrica e `RunOnceAsync` devem devolver prontamente seus objetos
e Tasks. Uma instância do serviço corresponde a uma execução do worker.

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
é aguardada integralmente antes de qualquer aquisição seguinte. Uma quiescência que
falha é observada e registrada como crítica, e encerra as aquisições.

`StopAsync` transmite o cancelamento cooperativo pelo mecanismo de `BackgroundService`.
O token recebido de quem chama `StopAsync` representa o prazo de shutdown do host.
Seu vencimento limita essa espera e produz um registro crítico quando `ExecuteTask`
ainda está pendente. O orçamento de shutdown do coordenador continua independente.

Mesmo depois de `StopAsync` retornar e `Dispose` ser chamado, `ExecuteTask` mantém a
supervisão da chamada em andamento e da quiescência devolvida por ela. O descarte
assíncrono das dependências ocorre depois dessa observação. Enquanto houver tarefas
pendentes, o worker mantém os recursos e suas referências. A ausência permanente de
quiescência conserva essa retenção até o término do processo. O host ou responsável
pelo lifetime pode acompanhar a supervisão pela propriedade pública `ExecuteTask`.

Esse comportamento limita o tempo aguardado pelo host, sob as condições usuais de
agendamento do runtime e implementações assíncronas que devolvem Tasks prontamente.
A supervisão só persiste enquanto o processo estiver vivo. Cancelar cooperativamente
ou observar o término local de uma Task deixa os efeitos HTTP remotos sujeitos ao
comportamento do sistema remoto. Exactly-once e interrupção de operações remotas
permanecem sem garantia.

Exceções que escapam de `RunOnceAsync` ficam em `ExecuteTask` e seguem a supervisão de
falhas do host. Os resultados esperados do coordenador são tratados pelas políticas
acima. O comportamento de falha do host depende de sua configuração.

## Verificação local em 15/11/2026

Base conferida em `teste-pr`, HEAD `b1ff6d725d4265dc55b8b36e924af1635e790056`.
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

Resultados finais das suítes executadas, todos aprovados e sem skips.

- Hosting teve 27 testes aprovados.
- Application teve 71 testes aprovados.
- API teve 38 testes aprovados.
- Domain teve 7 testes aprovados.
- Contracts teve 16 testes aprovados.
- Integrations.UberEats teve 120 testes aprovados.

Os 27 testes de Hosting usam tarefas sinalizadas e temporizadores de disparo explícito.
O relógio monotônico do helper compartilhado é controlado pelo teste. Os limites reais
de 10 segundos são guardas contra deadlock. Os testes cobrem políticas de polling,
backoff e reinicialização por cada status, configurações, isolamento entre workers,
owners, proteção contra resultados síncronos e sequências rápidas, cancelamento durante
espera, quiescência pendente ou falha e retenção até o término de tarefas tardias.
Dois casos exercitam o coordenador real com aquisição simulada que ignora cancelamento,
atravessam os dois prazos de shutdown e verificam descarte somente após término ou falha
tardia. Outro caso verifica que uma falha do logger preserva a observação da quiescência
e a retenção das dependências. Os demais casos simulam `RunOnceAsync` diretamente.

O restore inicial precisou de execução autorizada fora do sandbox para acessar
`NuGet.Config`. Um teste HTTP falhou inicialmente por bloqueio de escrita no log de
eventos do Windows e passou na reexecução autorizada da suíte. Uma compilação paralela
dos projetos API e UberEats disputou a mesma DLL. A suíte UberEats passou na execução
sequencial seguinte.

Os testes de Hosting foram adicionados ao job `test` da CI. A execução remota desse
workflow, um Generic Host completo, PostgreSQL real, HTTP remoto e processamento de
negócio permanecem sem verificação neste incremento. As garantias de lifetime dependem
da composição cumprir a transferência de posse documentada acima.
