# Coordenador de uma notificação da Inbox

`InboxProcessingCoordinator` pertence a Application e executa uma única tentativa de aquisição.
Reutiliza `IInboxProcessingStore` sem alterar suas garantias e recebe um
`IInboxNotificationProcessor`. Não há ativação na API, polling ou `BackgroundService`.
As garantias persistidas continuam descritas em [leases e fencing](inbox-processing-leases.md).

## Uso implementado

```csharp
var options = new InboxProcessingOptions(
    leaseDuration: TimeSpan.FromSeconds(30),
    renewalInterval: TimeSpan.FromSeconds(7),
    storeOperationTimeout: TimeSpan.FromSeconds(5),
    shutdownTimeout: TimeSpan.FromSeconds(3));
var coordinator = new InboxProcessingCoordinator(store, processor, options, TimeProvider.System);
var result = await coordinator.RunOnceAsync(Guid.NewGuid().ToString("N"), stoppingToken);
```

O chamador fornece uma identidade nova para cada execução do worker. Somente `Acquired` com
conteúdo confirmado inicia o processador. `NoEligibleWork` não significa Inbox vazia;
`OutcomeUnknown` não autoriza processamento, mesmo que uma implementação exponha conteúdo.
Não há nova aquisição dentro dessa execução.

O processador recebe o envelope normalizado e um token cooperativo. Seu resultado é:

- `Succeeded`: solicitar `CompleteAsync` uma vez.
- `RetryAfter(delay)`: solicitar `ScheduleRetryAsync` uma vez, depois de o processador terminar.
  Esse resultado é uma afirmação explícita do processador de que outra tentativa é segura,
  inclusive quanto aos efeitos externos. O delay respeita os limites já existentes do store.
- `OutcomeUnknown`: encerrar sem concluir nem reagendar.

Exceções, inclusive cancelamento não solicitado do processador, são `ProcessingFailed`.
Não são convertidas em retry. Exceções do store são `StoreFailed`, conforme a fronteira de
falhas do contrato existente. `Stage` e `Error` identificam a operação e a falha observada.
Nenhum desses resultados afirma que efeitos externos foram desfeitos.

## Sincronização e autoridade

Um único supervisor decide quais chamadas iniciar. O processamento e uma renovação podem
permanecer em voo simultaneamente. Há no máximo uma renovação em andamento; o próximo intervalo
começa depois da confirmação da anterior. `TimeProvider` controla intervalos e prazos.

`LeaseLost`, `OutcomeUnknown`, exceção ou timeout de renovação encerram a tentativa e pedem
cancelamento ao processador. Se esses resultados forem observados antes da finalização,
nenhuma conclusão ou solicitação de retry será iniciada, mesmo que o processador termine
depois com sucesso. A reserva fica sujeita à recuperação persistida por expiração.

Quando o processador termina, nenhuma nova renovação é iniciada. Uma renovação já em voo pode
concorrer com a finalização. Nesse caso o supervisor aguarda a validação atômica da própria
finalização: `CompleteAsync` confirmado prevalece sobre uma renovação tardia rejeitada, pois a
conclusão pode ter removido o lease. A mesma regra vale para retry confirmado. Uma conclusão
incerta permanece incerta, qualquer que seja o resultado da renovação; não há repetição da
conclusão, do retry ou dos efeitos de negócio.

O prazo local de cada chamada ao store limita a espera por confirmação. Timeout produz
`OutcomeUnknown`, pede cancelamento e não comprova rollback. Intervalos e prazos aceitam no
mínimo 1 ms, evitando o arredondamento de `Task.Delay` para uma espera nula. O intervalo de
renovação mais o prazo do store deve ser menor que a duração do lease. Essa margem é uma
política conservadora de abandono, não uma prova de autoridade: nenhuma decisão de propriedade
usa `ExpiresAtUtc` ou o relógio local. O store continua validando cada mutação.

Chamadas confirmadas são observáveis antes de cancelar seus temporizadores de prazo; um delay
cancelado também é uma Task concluída e não pode ser confundido com timeout da própria chamada.
Tarefas históricas já observadas são removidas do conjunto de acompanhamento a cada nova chamada.

## Cancelamento e encerramento limitado

- Cancelamento anterior à execução retorna `Cancelled` sem adquirir.
- Abandono de aquisição em voo retorna `OutcomeUnknown`, sem iniciar processamento caso a
  aquisição venha a confirmar depois.
- Cancelamento durante processamento retorna `Cancelled`, sem liberar/reagendar a reserva.
- Cancelamento ou timeout durante finalização retorna `OutcomeUnknown`; uma confirmação de
  sucesso recebida durante a drenagem ainda prevalece.

O encerramento pede cancelamento com `CancelAsync` e espera por todas as chamadas iniciadas e
callbacks de cancelamento, até `ShutdownTimeout`. Isso também limita a espera por um callback
que bloqueie. `ShutdownTimedOut` sinaliza que ainda pode haver código em execução; não autoriza
descarte das dependências que esse código usa nem repetição de efeitos.

`Quiescence` permite acompanhar a saída das Tasks iniciadas e dos callbacks. Falhas tardias são
observadas internamente; não alteram o resultado retornado nem são expostas como novos erros
nesse snapshot. Não se iniciam outras chamadas ao store após o retorno. Uma confirmação recebida
depois do prazo de encerramento também não modifica o snapshot.

## Verificação local — 2026-11-13

Os testes do coordenador usam processador e store sintéticos, `TaskCompletionSource` e
temporizadores de disparo explícito. Não usam sleeps para ordenar corridas. As esperas reais de
10 segundos são apenas guardas contra deadlock do teste. Os 45 casos novos cobrem aquisição
pendente/confirmada/ausente/incerta, renovação periódica sem sobreposição, perda e incerteza de
autoridade, conclusão, retry explícito, exceções síncronas e assíncronas, corridas entre renovação
e finalização, timeouts, cancelamento cooperativo e participantes/callbacks que não encerram.

Comandos executados em Release, target `net8.0`, SDK 10.0.101 e runtime .NET 8:

```powershell
dotnet test Ordering.Application.Tests/Ordering.Application.Tests.csproj -c Release --no-restore
dotnet test Ordering.API.Tests/Ordering.API.Tests.csproj -c Release --no-restore
dotnet test Ordering.Dominio.Tests/Ordering.Dominio.Tests.csproj -c Release --no-restore
dotnet test Ordering.Contracts.Partners.Tests/Ordering.Contracts.Partners.Tests.csproj -c Release --no-restore
dotnet test Ordering.Integrations.UberEats.Tests/Ordering.Integrations.UberEats.Tests.csproj -c Release --no-restore
dotnet build Ordering.Infrastructure.PostgreSql/Ordering.Infrastructure.PostgreSql.csproj -c Release --no-restore
```

Resultados: Application 59, API 38, Domain 7, Contracts 16 e Integrations.UberEats 120 aprovados,
totalizando 240 testes, sem falhas ou skips na verificação final. O teste HTTP de sanitização
inicialmente falhou pelo bloqueio do log de eventos do Windows no sandbox; os 38 testes da API
passaram na reexecução autorizada fora do sandbox. O build de Infrastructure passou sem avisos
ou erros. A revisão incluiu `git diff --check`.

## Limites

Não foi executada a suíte de integração PostgreSQL neste incremento; `INBOX_TEST_CONNECTION`
não estava configurada. O coordenador foi verificado com doubles, sem validação adicional de
suas corridas contra banco real. O workflow remoto de CI também não foi executado nesta sessão.

O limite de encerramento pressupõe implementações assíncronas que devolvam a Task prontamente
e disponibilidade de agendamento do runtime. Não há interrupção forçada de código síncrono,
garantia de tempo real ou término obrigatório de uma chamada externa. O processador deve incluir
todas as suas atividades na Task devolvida. Mesmo sua saída não prova que um servidor remoto
parou de produzir efeitos.

Não há garantia de exactly-once, idempotência externa, reconciliação automática de resultados
incertos ou processador de negócio real. Uma reserva expirada pode ser adquirida por outro worker
enquanto efeitos externos antigos ainda existem. POS, Uber Eats real, Kafka, Outbox e worker
contínuo permanecem fora deste incremento. Migrações, recebimento HTTP e publicação permanecem
com suas garantias anteriores.
