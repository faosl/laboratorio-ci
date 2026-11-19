# Transactional Outbox — incremento 3: Kafka real, Relay one-shot

Baseline: `3f770e97eafc38e9d12ad3fad6caf63cc19a389a`, branch `main`, .NET 8.

## Contratos e composição explícita

`Ordering.Application.Outbox.IOutboxEventPublisher` aceita o `OutboxMessage` persistido
e retorna `OutboxPublicationResult`. `Acknowledged` exige confirmação de entrega;
`Unconfirmed` não prova ausência de publicação. `OutboxDeliveryReceipt` contém tópico,
partição e offset para diagnóstico, sem comprovar consumo por destinatários.

`OutboxRelay.RunOnceAsync` executa no máximo uma aquisição e um envio lógico. Não chama
`RenewAsync`, não faz polling nem repete automaticamente aquisições, envios ou mutações.
Reutiliza integralmente `IOutboxProcessingStore`/`PostgresOutboxProcessingStore`.
Aquisição indeterminada não fornece trabalho. Somente `OrderConfirmed` é aceito.
Um evento não suportado produz `UnsupportedEvent` e mantém a reserva até expirar;
requer corrigir a origem/configuração antes de novas tentativas explícitas.

`Ordering.Infrastructure.Kafka` referencia Application e Confluent.Kafka 2.12.0.
Application não referencia Kafka nem Npgsql. Domain, Contracts, Inbox e migrações 001–004
permanecem inalterados. A API não referencia nem ativa o produtor/Relay.

Exemplo em uma composição de laboratório que já possua o `NpgsqlDataSource source`:

```csharp
await using var publisher = new KafkaOutboxEventPublisher(new()
{
    BootstrapServers = bootstrapServers, // fornecido explicitamente pelo laboratório
    Topic = topic,
    DeliveryTimeout = TimeSpan.FromSeconds(30)
});
var relay = new OutboxRelay(new PostgresOutboxProcessingStore(source), publisher, new()
{
    LeaseDuration = TimeSpan.FromSeconds(60),
    ExecutionTimeout = TimeSpan.FromSeconds(40),
    LeaseSafetyMargin = TimeSpan.FromSeconds(10),
    RetryDelay = TimeSpan.FromSeconds(5)
});
var result = await relay.RunOnceAsync(Guid.NewGuid().ToString("N"), cancellationToken);
Console.WriteLine($"{result.Status} {result.Stage} {result.EventId} {result.Delivery} {result.Receipt} {result.ErrorCode}");
```

Cada owner identifica uma execução; não substitui `EventId`. Crie previamente o tópico.
O adaptador não permite configuração arbitrária que possa desativar as garantias fixadas.
Esta composição se destina ao broker isolado do laboratório; não oferece configuração
de autenticação de produção. Não forneça segredos em código ou logs.

## Envelope e produtor

- Topic: obrigatório, explícito e validado; criação automática pelo cliente desativada.
- Key: `AggregateId`/`OrderId`, serializado em UTF-8.
- Value: bytes UTF-8 do `Payload` lido do JSONB persistido, sem novo envelope/serialização.
- Headers UTF-8: `event-id` = UUID no formato `D`; `event-type` = `OrderConfirmed`.
- `Acks.All`, `EnableIdempotence=true`, `MaxInFlight=5`,
  `MessageSendMaxRetries=int.MaxValue`, `RetryBackoffMs=100`, `LingerMs=0`.
- `MessageTimeoutMs`: configurável de 1 segundo a 5 minutos, com milissegundos inteiros;
  limita inclusive os retries de transporte. `RequestTimeoutMs` é o menor entre 5 segundos
  e metade do timeout de entrega. Não há retry adicional no Relay.

O adaptador aguarda `ProduceAsync` e só aceita `PersistenceStatus.Persisted` como ACK.
Erro Kafka é retornado como `Unconfirmed`, com código técnico, sem mensagem do provider.
Logs nativos do produtor são suprimidos para evitar exposição de detalhes de conexão;
o chamador deve registrar o resultado/receipt. Idempotência protege retries do protocolo
na sessão do produtor; não deduplica uma nova aquisição, sessão ou publicação lógica.

## Autoridade, limites e cancelamento

É obrigatório `DeliveryTimeout < ExecutionTimeout <= LeaseDuration - LeaseSafetyMargin`,
com margem positiva. Lease e atraso de retry respeitam os limites do protocolo existente.
O prazo de execução inclui aquisição, envio e conclusão/retry. Uma aquisição lenta pode
consumir parte desse prazo. Os limites são cooperativos, não garantias de tempo real:
pausas, atrasos de rede, scheduler ou falhas podem expirar a reserva antes da conclusão.

Nenhuma transação PostgreSQL fica aberta durante o envio. Após ACK, `MarkPublishedAsync`
continua validando atomicamente geração, owner, estado e expiração no relógio do banco.
`LeaseLost` não causa repetição com a geração antiga. Fencing não retrai mensagens Kafka.
Em falha sem ACK, apenas `ScheduleRetryAsync` pode validar e alterar atomicamente uma
reserva ainda válida. Um relógio local não concede autoridade.

Cancelamento antes do enqueue impede o envio. Depois do enqueue, o produtor continua
supervisionado até o delivery report limitado por `MessageTimeoutMs`; a Task nativa
não recebe o token cancelável do chamador. Assim, o retorno pode ultrapassar o prazo
cooperativo de execução. O Relay preserva evidência de ACK mesmo se retornar `Cancelled`
e deixa a recuperação para a elegibilidade persistida/expiração, sem mutação com token
cancelado. `Dispose`/`DisposeAsync` aguardam o envio supervisionado antes de destruir o
produtor. Não há Task de envio abandonada nem promessa de desfazer publicação ao cancelar.

## Resultados e recuperação

- `Published`: ACK Kafka e `MarkPublishedAsync` confirmado no PostgreSQL.
- `NoEligibleWork`: nenhum lease utilizável; nenhum envio (não prova Outbox vazia).
- `AcquisitionUnknown`: commit da aquisição incerto; nenhum envio autorizado.
- `RetryScheduled`: entrega não confirmada e retry confirmado localmente.
- `LeaseLost`: conclusão ou retry rejeitado atomicamente; nenhuma segunda mutação.
- `CompletionUnknown`: ACK conhecido, commit de `Published` incerto; não reagendar cegamente.
- `RetryUnknown`: entrega não confirmada, commit do agendamento incerto.
- `Cancelled`, `OperationalFailure`, `UnsupportedEvent`: estágio e evidência de entrega
  preservados; recuperação posterior depende do estado persistido. Falhas operacionais
  retornam o nome do tipo da exceção, sem seus detalhes de conexão.

Falha/timeout Kafka não autoriza `Published`, mesmo se houver publicação no broker.
ACK seguido de falha antes de concluir localmente permite nova aquisição após expiração
e outra publicação com o mesmo `EventId` e payload. Isso oferece publicação at-least-once
sob tentativas posteriores e disponibilidade, com possibilidade de duplicatas;
não há exactly-once entre PostgreSQL e Kafka nem deduplicação de consumidores implementada.

## Testes reais e CI

`Ordering.Infrastructure.PostgreSql.Tests/KafkaOutboxRelayTests.cs` combina a fixture
PostgreSQL existente (`INBOX_TEST_CONNECTION`, bancos isolados, migrações reais) com
Testcontainers.Kafka 4.8.1 e `confluentinc/cp-kafka:7.6.1` descartável, portas dinâmicas
e tópico exclusivo por teste. O consumidor existe somente nos testes, lê bytes, chave,
headers e offsets, e não integra POS ou negócio. Docker Linux é obrigatório. Ausência
de conexão PostgreSQL, Docker ou broker causa falha; não há skip silencioso.

A falha real de entrega usa tópico com `min.insync.replicas=2` em broker de uma réplica;
`acks=all` não pode ser satisfeito. Após reduzir para 1 e tornar o retry elegível, uma
nova chamada explícita recupera o evento. Isso verifica rejeição e recuperação funcional,
sem demonstrar replicação com duas réplicas em ISR.

Os testes também verificam Outbox sem trabalho, ACK seguido de falha local e duplicação
observável com produtor reiniciado, commit/rollback PostgreSQL reais com resposta perdida,
aquisição indeterminada sem envio, fencing após aquisição sucessora e dois Relays com
fontes/produtores independentes. Barreiras controlam a concorrência; `FOR UPDATE NOWAIT`
comprova liberação do lock durante o envio. Cancelamento e descarte são exercitados com
envio real ainda pendente. As falhas de resposta são injetadas em fronteiras controladas;
não representam perda física de pacotes ou crash real de processos.

A CI executa essa classe no projeto PostgreSQL sem filtro/skip, no job `test` (15 minutos),
com Docker disponível no runner Ubuntu. Somente nome da etapa de testes e seu orçamento
foram ajustados. As políticas `docker`, `attest`, `publish` e `ENABLE_IMAGE_PUBLISH`
permanecem inalteradas.

Não foram implementados worker contínuo, renovação periódica, consumidor de produção,
Kafka Transactions, DLQ, sequência/ordenação multi-evento ou integração POS. Broker
single-node não valida perda definitiva de líder, failover ou tolerância a perda física.

## Verificação local — 19/11/2026

SDK 10.0.101, alvo .NET 8/runtime 8.0.26, PostgreSQL 17.7 real e Kafka 7.6.1 em
containers Linux descartáveis no Docker Desktop. A conexão PostgreSQL foi fornecida
por `INBOX_TEST_CONNECTION`, restrita ao loopback local; nenhum segredo foi adicionado.
Os bancos isolados foram criados/removidos pela fixture e o broker pelo Testcontainers.

Comandos de preparação/compilação e teste específico:

```powershell
dotnet restore Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj
dotnet build Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj -c Release --no-restore
dotnet test Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj -c Release --filter FullyQualifiedName~KafkaOutboxRelayTests --logger 'console;verbosity=normal' --logger 'trx;LogFileName=kafka-outbox.trx' --results-directory "$env:TEMP\ordering-outbox-increment3-tests"
```

A compilação inicial detectou uso incorreto do construtor de `ConfigEntry` no teste;
foi corrigido para inicialização por propriedades. A compilação final dos projetos,
executada pelo `dotnet test`, passou sem avisos/erros. A primeira restauração encontrou
restrição de acesso à configuração NuGet no sandbox; as execuções finais usaram acesso
autorizado. A suíte específica passou com **14 aprovados, zero falhas/ignorados**.

Regressão completa executada:

```powershell
$testProjects = @('Ordering.Application.Tests', 'Ordering.Infrastructure.PostgreSql.Tests', 'Ordering.Hosting.Tests', 'Ordering.Dominio.Tests', 'Ordering.Contracts.Partners.Tests', 'Ordering.Integrations.UberEats.Tests', 'Ordering.API.Tests')
foreach ($project in $testProjects) {
    dotnet test "$project/$project.csproj" -c Release --logger 'console;verbosity=minimal' --logger "trx;LogFileName=$project.trx" --results-directory "$env:TEMP\ordering-outbox-increment3-tests"
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
}
docker run --rm --volume 'C:\EstudosDevOps:/repo:ro' --workdir /repo rhysd/actionlint:1.7.12 -color
git diff --check
```

- Application: **95 aprovados**, incluindo 18 casos do Relay.
- PostgreSQL: **174 aprovados**, incluindo os 14 casos Kafka reais.
- Hosting: **51 aprovados**; Domain: **7**; Contracts: **16**.
- Integração Uber Eats: **120 aprovados**; API: **38**.
- Total da regressão: **501 aprovados, zero falhas, zero ignorados**.
- `actionlint` e `git diff --check`: aprovados.

Os arquivos TRX ficam em `%TEMP%\ordering-outbox-increment3-tests`. O teste de caminho
feliz observou `event-id=66a0213c-b7ca-46a9-a1fb-a4f1ca4a18fc`, partição 0/offset 0,
e verificou `Published` persistido no banco. O teste de falha após ACK observou
`event-id=c5ca5678-2cd9-49fe-b81d-ad95c8cd0211` no mesmo tópico, partição 0,
nos offsets **0 e 1**, com payload/headers idênticos e conclusão local apenas depois
da recuperação. Esses exemplos são da execução específica, anterior à regressão.

Arquivos desta alteração:

- `Ordering.Application/Outbox/IOutboxEventPublisher.cs` e `OutboxRelay.cs`.
- `Ordering.Infrastructure.Kafka/Ordering.Infrastructure.Kafka.csproj` e `KafkaOutboxEventPublisher.cs`.
- `Ordering.Application.Tests/OutboxRelayTests.cs`.
- `Ordering.Infrastructure.PostgreSql.Tests/KafkaOutboxRelayTests.cs` e o respectivo `.csproj`.
- `.github/workflows/ordering-api.yml`, `README.md` e este documento.

O SHA final e a execução da CI correspondente são apresentados no relatório de entrega.
Os números acima são resultados locais; sucesso da baseline não constitui evidência
de execução remota dos novos testes.

Referências do cliente: [delivery report e ProduceAsync](https://docs.confluent.io/kafka-clients/dotnet/current/overview.html),
[configuração idempotente](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.ProducerConfig.html)
e [PersistenceStatus](https://docs.confluent.io/platform/current/clients/confluent-kafka-dotnet/_site/api/Confluent.Kafka.PersistenceStatus.html).
