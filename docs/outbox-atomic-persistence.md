# Transactional Outbox — incremento 1: persistência atômica

Este laboratório comprova **atomicidade local no PostgreSQL**: a transição de um pedido
`Pending` para `Confirmed` e a persistência de `OrderConfirmed` são confirmadas na mesma
transação. **Não comprova entrega ao Kafka nem comportamento exatamente uma vez.**
Não há Publisher, relay, polling da Outbox, processamento de eventos ou endpoint novo.

## Modelo e composição

`Ordering.Application.Outbox.IOrderConfirmationStore` define a operação atômica e
`ConfirmOrderUseCase` a expõe sem dependências de infraestrutura. A implementação
`PostgresOrderConfirmationStore` recebe um `NpgsqlDataSource` explicitamente. Nenhum host
passa a executar confirmações automaticamente neste incremento.

A migração aditiva `003_outbox_lab.sql` cria tabelas persistentes exclusivas do laboratório:

- `ordering_lab_orders`: `order_id` e estado de negócio `Pending` ou `Confirmed`.
- `ordering_outbox_messages`: `event_id` UUID único, `event_type = 'OrderConfirmed'`,
  `aggregate_id` referenciando o pedido, `payload` JSONB, `publication_status = 'Pending'`
  e `created_at_utc` gerado pelo banco.

O payload contém `EventId` e `OrderId`; o identificador é o mesmo da coluna `event_id`.
Estado de negócio, estado de publicação e estados de coordenação da Inbox são conceitos
independentes. `Ordering.Dominio.Order` e contratos específicos de parceiros permanecem
inalterados. Não há criação de pedidos pelo caso de uso: os testes provisionam seus pedidos
diretamente no banco.

`InboxSchema.ApplyAsync` aplica a versão 003 pelo mecanismo existente: versões pendentes
em ordem, com advisory lock e registro da versão na mesma transação de provisionamento.
001 e 002 são imutáveis. Reaplicar preserva dados e datas do histórico de versões.
O comando explícito existente `Ordering.Inbox.TestHost migrate` também provisiona a versão 003;
a API não executa migrações no startup.

## Transação e resultados

Cada confirmação abre uma conexão e uma transação `READ COMMITTED`, requer um primary
com `fsync` e `full_page_writes` habilitados e força `synchronous_commit = on` localmente.
Executa `UPDATE ... WHERE order_id = $1 AND status = 'Pending' RETURNING order_id`.
Somente uma transição elegível gera um UUID e insere o evento usando a mesma conexão e
transação, antes de um único commit.

- `Confirmed`: commit confirmado; retorna o `EventId` persistido.
- `NoEligibleTransition`: pedido inexistente ou com estado incompatível. Não insere evento.
- `OutcomeUnknown`: falha de comunicação, timeout ou cancelamento na fronteira de commit.
  Não retorna um `EventId` como confirmado; o servidor pode ter feito commit ou rollback.

Falhas anteriores ao commit propagam e o descarte da transação não confirma nenhuma das
alterações. Falhas transitórias do provider anteriores ao commit são traduzidas para
`OrderConfirmationStorageUnavailableException`, sem expor Npgsql na Application.
Não há retry automático. Após perda da resposta, uma leitura sem evento pode observar uma
transação ainda em andamento e não comprova rollback. Um retry com `NoEligibleTransition`
também não comprova rollback da tentativa anterior.

Após um commit, nova confirmação não altera o pedido nem o evento existente: `EventId`,
payload, tipo, agregado, estado de publicação e data são preservados. Confirmações concorrentes
disputam o mesmo row lock; após o vencedor confirmar, o PostgreSQL reavalia o predicado
`Pending` e o concorrente não realiza outra transição. Esta garantia se refere à operação
do laboratório; alterações administrativas diretas no estado não fazem parte do protocolo.

## Execução e evidências

Reutiliza `DatabaseFixture` e `INBOX_TEST_CONNECTION`; os testes criam e removem bancos
isolados. A conexão deve apontar para PostgreSQL real descartável, com permissão `CREATEDB`.
A ausência da variável falha explicitamente, sem ignorar testes.
Consulte [a configuração local existente](inbox-postgresql.md).

```powershell
dotnet test Ordering.Infrastructure.PostgreSql.Tests -c Release --filter FullyQualifiedName~PostgresOrderConfirmationTests
dotnet test Ordering.Infrastructure.PostgreSql.Tests -c Release
git diff --check
```

Os testes inspecionam as duas escritas dentro da transação e sua invisibilidade em outra
conexão, injetam falha antes do commit e verificam rollback das duas alterações. Cobrem
sucesso, preservação em retry, ausência/incompatibilidade, UUID único, cancelamento,
configuração de durabilidade, migração repetível e upgrade real da versão 002 com dados
preexistentes da Inbox preservados.

A concorrência usa `TaskCompletionSource` e observação de `pg_stat_activity` /
`pg_blocking_pids`: o primeiro commit só é liberado após comprovar que o segundo UPDATE
aguarda seu bloqueio. Não há `Thread.Sleep` nem suposição de concorrência baseada em tempo.
O pequeno intervalo entre consultas só limita a frequência de observação do banco.

A perda de resposta é injetada após commit real e após rollback real, para IOException,
timeout, cancelamento e erro do provider. Ambos retornam `OutcomeUnknown`; consultas
independentes verificam o resultado efetivo e o retry preserva o evento quando houve commit.
Outro teste mantém o commit pendente e comprova que ausência de evento não prova rollback.

Essas injeções verificam a classificação da incerteza, sem simular perda física de pacotes.
Queda de energia, honestidade do armazenamento, recuperação após crash do servidor,
replicação/failover e carga de produção não foram verificados. A persistência local não
equivale a publicação ou consumo; entrega ao Kafka e efeitos externos exatamente uma vez
permanecem fora deste incremento. Workflows e políticas de publicação não foram alterados.

## Verificação local — 18/11/2026

Execução em Release, com projetos `net8.0`, SDK 10.0.101 e runtime .NET 8.0.26.
PostgreSQL real 17.7 em container descartável, exposto somente em `127.0.0.1:55433`,
com `fsync`, `full_page_writes` e `synchronous_commit` habilitados.

- `Ordering.Infrastructure.PostgreSql.Tests`: 97 aprovados, incluindo 20 casos de Outbox.
- `Ordering.Dominio.Tests`: 7 aprovados.
- `Ordering.Application.Tests`: 71 aprovados.
- `Ordering.Hosting.Tests`: 51 aprovados.
- `Ordering.Contracts.Partners.Tests`: 16 aprovados.
- `Ordering.Integrations.UberEats.Tests`: 120 aprovados.
- `Ordering.API.Tests`: 38 aprovados.

Total da execução final: **400 aprovados, 0 falhas, 0 ignorados**. Cada projeto foi executado
com `dotnet test <projeto>/<projeto>.csproj -c Release`. A compilação final não apresentou
avisos nem erros. `git diff --check` passou. O sandbox inicialmente bloqueou a configuração
do NuGet; a execução final usou acesso autorizado fora do sandbox.
O workflow remoto de CI não foi executado nesta sessão.
