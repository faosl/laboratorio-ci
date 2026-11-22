# Transactional Outbox — incremento 6: observabilidade do Relay

Referência: `f39be7c3bb76ac45866fa19d225d9cf037bc42d3`, branch `main`, alvo .NET 8.

`OutboxRelayDiagnostics` centraliza os instrumentos de `System.Diagnostics.Metrics`
e a `ActivitySource`. Ambos usam o nome estável `Ordering.Application.Outbox`.
Não há dependência adicional de OpenTelemetry, exporter, collector ou servidor.
Um consumidor pode assinar o Meter com `MeterListener` e os traces com
`ActivityListener`; sem listeners, não é necessário configurar infraestrutura.
As Activities respeitam a amostragem do listener e só são criadas quando habilitadas.

## Instrumentos e limites de medição

Todos os contadores são `Counter<long>` e recebem incremento 1 por observação.
Todos os histogramas são `Histogram<double>`, com unidade `s` (segundos).
As durações usam `Stopwatch.GetTimestamp`/`GetElapsedTime`, um relógio monotônico;
não usam o relógio de parede, o relógio do banco ou a expiração da lease.

- `outbox.kafka.ack.count` (unidade `{ack}`, sem tags): incrementa uma vez quando
  `PublishAsync` retorna `Acknowledged` com receipt não nulo. O receipt é a evidência
  exigida pelo contrato existente; não é feita nova consulta ao broker. A observação
  ocorre antes da checagem de cancelamento após o envio e antes de qualquer conclusão
  local. `Unconfirmed`, exceção e ACK sem receipt não incrementam o contador.
- `outbox.completion.count` (unidade `{result}`, tag `outbox.status`): incrementa
  somente quando uma chamada efetiva de `MarkPublishedAsync` retorna um resultado.
  Valores: `Succeeded`, `LeaseLost`, `OutcomeUnknown`. Não conta aquisição,
  reagendamento, chamada não iniciada, exceção ou cancelamento sem resultado retornado.
  O total deste contador **não** é um contador de publicações concluídas com sucesso:
  para a confirmação local, selecionar apenas `Succeeded`.
- `outbox.lease_lost.count` (unidade `{loss}`, tag `outbox.stage`): incrementa somente
  para resultado `LeaseLost` de mutação efetivamente retornada. Estágios: `Completion`
  (`MarkPublishedAsync`) e `Retry` (`ScheduleRetryAsync`). O Relay não renova leases.
  Expiração presumida pelo relógio local e `OutcomeUnknown` não contam como perda
  observada de autoridade.
- `outbox.publication.duration` (tag `outbox.status`): uma amostra por chamada efetiva
  de `PublishAsync`, do início da invocação ao retorno ou lançamento de exceção,
  incluindo a espera assíncrona. Status: `Acknowledged`, `Unconfirmed`, `NotAttempted`
  (caso devolvido pelo publisher), `InvalidReceipt` (ACK sem receipt), `Exception`
  ou `Cancelled`. `Acknowledged` exige receipt não nulo. Um token cancelado com ACK
  retornado continua sendo `Acknowledged` nesta chamada; o resultado da tentativa
  completa pode ser `Cancelled`.
- `outbox.completion.duration` (tag `outbox.status`): uma amostra por chamada efetiva
  de `MarkPublishedAsync`, do início da invocação ao retorno ou exceção, incluindo a
  espera assíncrona. Status: `Succeeded`, `LeaseLost`, `OutcomeUnknown`, `Exception`
  ou `Cancelled`. Uma amostra `Exception`/`Cancelled` prova que houve tentativa de
  conclusão, sem inventar resultado para `outbox.completion.count`.
- `outbox.relay.attempt.duration` (tags `outbox.status`, `outbox.stage`,
  `outbox.delivery`): uma amostra por execução de `RunOnceAsync`, da entrada até sua
  saída, incluindo validação, aquisição, publicação e mutação quando executadas.
  As tags correspondem aos nomes dos enums do `OutboxRelayResult`. Para argumento
  inválido que lança antes do tratamento operacional, registra `Exception`,
  `Acquisition`, `NotAttempted`; a exceção original continua propagada.

Nos instrumentos de operação, `Cancelled` significa `OperationCanceledException`
com o token da execução cancelado, seguindo o mesmo filtro do Relay. Uma exceção
de cancelamento sem esse token cancelado permanece `Exception` e produz o resultado
operacional já existente. Se a operação retorna um resultado mesmo após cancelamento
do token, a telemetria registra o resultado real, sem substituí-lo por cancelamento.

As durações de operação terminam imediatamente após o retorno observado ou na saída
por exceção. A duração total inclui o custo da própria instrumentação. Não há medição
adicional do intervalo entre ACK e conclusão neste incremento, nem inferência sobre
o instante em que o broker realizou internamente o append.

As tags agregadas são exclusivamente os status, estágio e delivery finitos descritos
acima. Não incluem EventId, LeaseGeneration, owner, tipo arbitrário de evento, payload,
AggregateId, tópico, partição, offset, ErrorCode, tipo ou mensagem da exceção.

## Correlação por tentativa

Cada execução inicia `outbox.relay.attempt` como Activity interna, respeitando um
eventual contexto pai. Chamadas efetivas geram Activities filhas, encerradas mesmo
em exceção: `outbox.acquire`, `outbox.publish`, `outbox.complete`, `outbox.retry`.
Não se cria span de publicação ou conclusão se a chamada não ocorreu.
As filhas usam o mesmo TraceId e o SpanId da tentativa como ParentSpanId.

A tentativa recebe `outbox.event_id` (UUID) e `outbox.lease_generation` (inteiro)
somente após aquisição confirmada com trabalho disponível. As filhas de publicação
e mutação também recebem esses atributos. A aquisição é correlacionada pela relação
pai/filho; aquisição incerta não atribui uma identidade presumida, mesmo que uma
implementação indevidamente devolva Work. Não há IDs de negócio nas métricas.

Cada filha registra `outbox.status`, com os mesmos nomes de resultado da operação
ou `Exception`/`Cancelled`; publicação usa também `InvalidReceipt`. Aquisição
`Acquired`/`NoEligibleWork`, publicação com ACK válido e mutação `Succeeded` usam
ActivityStatus `Ok`. Outros resultados usam `Error`. O span de publicação com ACK
válido recebe ainda o evento `outbox.kafka.ack`, antes da próxima checagem de
cancelamento. Nenhum evento de ACK é emitido para publicação não confirmada.

A Activity da tentativa recebe status, estágio e delivery do resultado do Relay.
`Published`, `NoEligibleWork` e `RetryScheduled` usam `Ok`; demais resultados e
exceções usam `Error`, sem mensagem de exceção. `Ok` em aquisição, reagendamento ou
ausência de trabalho representa o resultado daquela operação, não entrega Kafka.
Não são anexados payload, owner, receipt ou mensagens de erro aos traces.

## Interpretação e preservação do protocolo

ACK Kafka observado, tentativa de conclusão e resultado local são fatos separados:

- ACK seguido de `Succeeded`: há ACK e confirmação persistida local observada.
- ACK seguido de `LeaseLost`: há ACK e rejeição local por perda de autoridade;
  fencing não retrai a publicação Kafka.
- ACK seguido de `OutcomeUnknown`: há ACK, mas não se sabe se a conclusão local
  foi persistida. Não autoriza inferir rollback nem reagendar cegamente.
- ACK seguido de cancelamento antes da conclusão: há ACK e duração do envio,
  sem contador nem duração de conclusão e sem span `outbox.complete`.
- ACK seguido de exceção/cancelamento na conclusão: há ACK e duração da chamada
  de conclusão, sem contar um resultado de mutação que não foi retornado.
- Publicação não confirmada seguida de reagendamento: não há ACK nem conclusão;
  o span `outbox.retry` registra o resultado real da mutação.
- `NoEligibleWork`/aquisição incerta: não há publicação nem mutação. Ausência de
  trabalho elegível não prova uma Outbox vazia.

Contadores são observações locais em memória: não são ledger durável, não garantem
exportação em crash e não substituem o estado persistido. Subtrair ACKs e conclusões
não produz um backlog confiável. Retries posteriores podem observar mais de um ACK
para o mesmo EventId, preservando a possibilidade de duplicatas at-least-once.
Não há prova de consumo, exactly-once, throughput de publicação ou capacidade de
produção. As medidas de chamada não isolam tempo de rede, append ou commit interno.

Portas, enums, resultados, Publisher Kafka, Store PostgreSQL, SQL, migrações,
fencing, retry, polling, backoff, shutdown e timeouts permanecem como antes.
Os logs estruturados existentes no Hosting continuam sendo produzidos; Application
não ganha dependência de `Microsoft.Extensions.Logging`.

## Validação reproduzível

`OutboxRelayObservabilityTests.cs` reutiliza os doubles dos testes anteriores,
com `MeterListener` e `ActivityListener` descartados em cada teste. A coleção não
executa em paralelo com outras coleções para isolar os instrumentos globais.
Os testes verificam contagens exatas, chamadas e resultados de negócio, status,
parentesco dos spans e allowlists das tags. Durações precisam apenas ser finitas e
não negativas: não há duração mínima, sleep, limiar de desempenho ou collector.

```powershell
dotnet test Ordering.Application.Tests/Ordering.Application.Tests.csproj
dotnet test Ordering.Hosting.Tests/Ordering.Hosting.Tests.csproj
dotnet test Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj --filter 'FullyQualifiedName~PostgresOutboxProcessingTests|FullyQualifiedName~KafkaOutboxRelayTests'
git diff --check
```

As regressões de infraestrutura exigem `INBOX_TEST_CONNECTION` para PostgreSQL
real com permissão de criar bancos isolados; Kafka usa Testcontainers e Docker.
Ambiente ausente ou falha de infraestrutura deve ser reportado como limitação,
sem declarar esses comportamentos validados por doubles. Não há alteração de CI
nem execução de benchmark pesado neste incremento.

Validação executada em 22/11/2026 no Windows, SDK 10.0.101, alvo `net8.0`
e runtime .NET 8.0.26:

- Application: 115 aprovados, incluindo 20 casos de observabilidade; zero falhas
  ou testes ignorados.
- Hosting: 74 aprovados; zero falhas ou testes ignorados.
- Regressões reais filtradas acima: 80 aprovados (63 de processamento PostgreSQL,
  17 de Relay/worker Kafka); zero falhas ou testes ignorados. PostgreSQL
  `17.7-bookworm` em contêiner temporário com porta local aleatória; Kafka
  `confluentinc/cp-kafka:7.6.1` via Testcontainers. Contêineres removidos ao final.

Esses resultados validam os cenários funcionais e as observações locais testadas.
Exportação, armazenamento, dashboards, alertas e capacidade de produção não foram
avaliados. O SDK 8 não está instalado no host; a execução usou o runtime 8 com
compilação para `net8.0` pelo SDK disponível.
