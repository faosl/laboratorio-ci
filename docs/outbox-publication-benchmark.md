# Benchmark fim a fim da publicação Outbox

Ferramenta opt-in .NET 8: `Ordering.Outbox.PublicationBenchmarks`. Não integra a CI,
o startup HTTP nem os workers operacionais. Não modifica SQL, migrações, contratos,
ACKs, retries, idempotência, leases, fencing, pool ou cancelamento da implementação.

Executa o fluxo real por `OutboxRelay.RunOnceAsync`, com
`PostgresOutboxProcessingStore` e `KafkaOutboxEventPublisher`. Não há doubles de
publicação no benchmark normal. Um worker chama o relay sequencialmente.

## Execução PowerShell

Requisitos: SDK capaz de compilar `net8.0`, runtime .NET 8, Docker Engine Linux
acessível e PostgreSQL de laboratório com `CREATEDB`, `fsync=on` e
`full_page_writes=on`. Execute da raiz do checkout. O Kafka é criado automaticamente
por Testcontainers (`confluentinc/cp-kafka:7.6.1`); não recebe endereço de broker
externo. NuGet e Docker podem precisar baixar pacotes/imagens.

Exemplo de PostgreSQL **novo e descartável**, sem volume persistente, porta
aleatória exposta apenas em localhost e credencial temporária:

```powershell
$labName = 'outbox-publication-pg-' + [guid]::NewGuid().ToString('N')
$labPassword = [guid]::NewGuid().ToString('N')
$previousConnection = $env:INBOX_TEST_CONNECTION
try {
    docker run --detach --name $labName --publish '127.0.0.1::5432' `
        --env POSTGRES_USER=outbox_lab --env POSTGRES_DB=postgres `
        --env "POSTGRES_PASSWORD=$labPassword" postgres:17.7-bookworm
    if ($LASTEXITCODE -ne 0) { throw 'PostgreSQL container failed' }
    $ready = $false
    for ($i = 0; $i -lt 60; $i++) {
        docker exec $labName pg_isready -U outbox_lab -d postgres *> $null
        if ($LASTEXITCODE -eq 0) { $ready = $true; break }
        Start-Sleep -Seconds 1
    }
    if (!$ready) { throw 'PostgreSQL not ready' }
    $containerInfo = docker inspect $labName | ConvertFrom-Json
    $port = $containerInfo[0].NetworkSettings.Ports.'5432/tcp'[0].HostPort
    $env:INBOX_TEST_CONNECTION = "Host=127.0.0.1;Port=$port;Database=postgres;Username=outbox_lab;Password=$labPassword"
    dotnet test Ordering.Outbox.PublicationBenchmarks.Tests -c Release
    if ($LASTEXITCODE -ne 0) { throw 'Unit tests failed' }
    dotnet run --project Ordering.Outbox.PublicationBenchmarks -c Release -- `
        --smoke --output artifacts/publication-smoke.json
    if ($LASTEXITCODE -ne 0) { throw 'Smoke incomplete; inspect its JSON' }
    dotnet run --project Ordering.Outbox.PublicationBenchmarks -c Release --no-build -- `
        --output artifacts/publication-benchmark.json
    if ($LASTEXITCODE -ne 0) { throw 'Benchmark incomplete; inspect its JSON' }
} finally {
    $env:INBOX_TEST_CONNECTION = $previousConnection
    docker rm --force $labName
}
```

Use novos nomes de arquivo a cada execução: o console reserva o relatório com
`FileMode.CreateNew` e mantém esse handle; um arquivo existente causa erro antes de
conectar aos serviços. Omitir `--output` gera nome aleatório em `artifacts/`.
Não coloque credenciais no caminho do relatório. `INBOX_TEST_CONNECTION` é a única
origem da conexão PostgreSQL; nunca aponte para produção ou outros ambientes.

Smoke: 20 eventos, uma rodada. Padrão: 500 eventos por rodada, três rodadas.
Ambos incluem um warm-up de 20 eventos e um experimento de recuperação de um
evento. Os valores são experimentais, configuráveis, sem metas de produção:

```powershell
dotnet run --project Ordering.Outbox.PublicationBenchmarks -c Release -- `
    --events 500 --rounds 3 --warmups 1 --warmup-events 20 `
    --batch-timeout-seconds 300 --verification-timeout-seconds 30
```

`--help` não acessa serviços. As contagens/deadlines devem ser positivas. O limite
do lote cancela o token do relay existente; não encurta a supervisão de envio Kafka
já iniciado. Não há retries extras: qualquer resultado diferente de `Published`,
receipt inválido ou identidade inesperada interrompe o lote; o limite adicional
de tentativas é o tamanho do lote. Falha em uma fase interrompe as fases seguintes.
Código de saída 0 exige todas as fases e cleanup confirmados; 1 indica execução
incompleta. Ctrl+C solicita cancelamento cooperativo.

## Isolamento e preparação

Cada fase, incluindo cada rodada e warm-up, cria um banco aleatório
`outbox_pub_<guid>` com `TEMPLATE template0`, aplica `InboxSchema.ApplyAsync` e
cria um tópico aleatório, com uma partição e uma réplica. O banco fornecido na
variável só serve como conexão administrativa para CREATE/DROP do nome gerado.
Não há TRUNCATE nem alteração de tabelas em bancos preexistentes.

Insere pedidos `Pending` e chama `ConfirmOrderUseCase` com
`PostgresOrderConfirmationStore`, preservando a confirmação e Outbox atômicas.
Registra os EventIds confirmados e consulta os registros `Pending` antes da
medição. Não fabrica eventos nem recicla registros `Published`. A massa, o schema
novo, o tópico e um producer novo por fase restauram condições lógicas equivalentes.
Não há limpeza de caches do sistema operacional/PostgreSQL; o broker é compartilhado
entre as fases. A primeira conexão/metadata do producer de cada fase pode contribuir
para o lote medido. Não se faz tuning ou pré-conexão artificial do producer.

## Medição e verificação

O relógio monotônico cobre o lote finito de chamadas sequenciais completas ao relay
e seu pequeno custo de coordenação. Preparação, gravação de checkpoints JSON e
consultas de verificação ficam fora desse intervalo. As latências por tentativa
medem a chamada completa a `RunOnceAsync`. `MeterListener` observa diretamente os
histogramas existentes de publicação, conclusão e tentativa, convertendo segundos
em milissegundos. Não existe histograma de aquisição no contrato atual; o console
não altera a instrumentação para acrescentá-lo.

O JSON conserva todas as amostras e resultados por tentativa: identidade, duração,
status do relay e delivery, estágio, receipt, sua validade e indicador de resultado
incerto. Mensagens de exceção não são registradas. p50/p95/p99 usam nearest-rank
das amostras observadas de cada distribuição; percentis de estágios não são somados.

Após parar o relógio, consulta todas as identidades e seus `publication_status`
persistidos no banco exclusivo. Conta IDs distintos `Published` com timestamp de
publicação; não deriva conclusões subtraindo contadores. Um consumidor independente
lê a partição desde o início até EOF e o high watermark final, com deadline. Valida
key AggregateId, exatamente os headers `event-id` e `event-type` e os bytes do
payload persistido. Os registros do relatório contêm identidades e posições, sem
payload. A reconciliação confronta cada receipt com seu EventId/partição/offset,
detectando faltantes, inesperados, duplicados, envelopes divergentes, ACK sem estado
durável e conclusão local não confirmada. EOF não alcançado invalida a fase.

Cada rodada apresenta duração total, tentativas/s, ACKs/s e IDs distintos
duravelmente `Published`/s, com o mesmo denominador de tempo do lote. ACKs incluem
reconhecimento válido do broker; não provam consumo por aplicação. O tempo do
consumidor de verificação não entra no throughput. `MeasuredTotals` exclui warm-up
e recuperação; os percentis/throughputs são apresentados por rodada, sem mesclar
esses cenários. `BaselineValid=false`, `Status=Incomplete` ou
`Metrics.BaselineEligible=false` tornam números parciais apenas diagnósticos.

Os metadados incluem SHA Git, checkout modificado, hashes dos assemblies, runtime,
clientes, versão PostgreSQL por fase, imagem e versão Kafka obtida no contêiner.
Não são gravados senha, connection string, endereço de bootstrap ou payload.
O broker de uma réplica **não valida alta disponibilidade**. Throughput de lote
finito **não representa capacidade sustentada em produção**.

## Recuperação e duplicação

Fase `recovery`, excluída das métricas normais. Um decorator delega aquisição ao
store real e lança uma `IOException` controlada na entrada de `MarkPublishedAsync`,
depois do ACK real. É o padrão de
`AckThenLocalFailure_LeaseRecoveryCanPublishSameEventAgain`, sem modificar adapters.
Verifica primeiro um registro Kafka e estado `Processing`. Somente nesse banco
descartável, antecipa a expiração do lease por SQL, cria outro producer e chama o
relay novamente. Verifica dois registros Kafka, dois ACKs com offsets distintos,
um EventId e um único evento `Published` persistido.

Isso injeta uma falha; **não testa um crash real de processo**. Demonstra a janela
entre ACK Kafka e conclusão PostgreSQL, sem afirmar exactly-once. A idempotência do
producer não elimina uma nova publicação lógica após recuperação.

## Cleanup e falhas

O console tenta descartar cada pool e remover exclusivamente o banco que confirmou
ter criado; em `finally`, descarta o contêiner Kafka (os tópicos desaparecem junto
com ele). O JSON é atualizado antes de acessar serviços, após identificar recursos,
após preparar cada fase e ao terminá-la, incluindo exceções e cleanup. Em erro,
retém amostras já obtidas, sinaliza execução incompleta e registra somente tipos
de erro. O relatório final só é válido com cleanup confirmado.

`finally` e o Resource Reaper do Testcontainers não garantem limpeza após kill,
queda do host ou perda de conexão. Confira os campos `DatabaseCleanup` e
`KafkaCleanup`. Em `CreationOutcomeUnknown`, verifique manualmente se o CREATE
chegou ao servidor. O banco administrativo fornecido jamais deve ser removido.
Para recursos remanescentes, após confirmar que os nomes pertencem à execução:

```powershell
$report = Get-Content artifacts/publication-smoke.json -Raw | ConvertFrom-Json
$report.Phases | Select-Object Kind, Database, DatabaseCleanup
$report.Kafka | Select-Object ContainerName
# Se o contêiner Kafka da execução ainda existir:
docker rm --force $report.Kafka.ContainerName
# Em psql conectado ao PostgreSQL de laboratório, para CADA nome aleatório
# confirmado no relatório e no servidor (nunca o banco administrativo):
# DROP DATABASE "outbox_pub_<guid-exato-do-relatorio>" WITH (FORCE);
```

Os testes pequenos de reconciliação não precisam de Docker/PostgreSQL. Regressões
de infraestrutura usam seus próprios fixtures e exigem ambos os serviços:

```powershell
dotnet test Ordering.Outbox.PublicationBenchmarks.Tests -c Release
dotnet test Ordering.Application.Tests -c Release
dotnet test Ordering.Hosting.Tests -c Release
dotnet test Ordering.Infrastructure.PostgreSql.Tests -c Release
git diff --check
```
