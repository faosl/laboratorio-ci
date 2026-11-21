# Incremento 5 — benchmark de aquisição da Transactional Outbox

Ferramenta explícita em `Ordering.Outbox.Benchmarks`, com .NET 8, Npgsql já utilizado no
repositório e `Stopwatch`. Não participa da CI nem do startup dos serviços. Não publica
eventos. A única alteração na biblioteca PostgreSQL concede acesso interno ao assembly
do benchmark, para reutilizar a constante `AcquisitionSql` sem copiar ou alterar a consulta.
As chamadas completas usam `PostgresOutboxProcessingStore.TryAcquireAsync` sem hooks.

## Execução reproduzível

Execute na raiz do checkout. Requer SDK compatível com .NET 8, runtime .NET 8, Git e
um PostgreSQL **exclusivamente de laboratório**, primary com `fsync=on` e
`full_page_writes=on`. O usuário precisa de `CREATEDB`. Não usar credenciais ou host de
produção. A ferramenta só lê a conexão de `INBOX_TEST_CONNECTION`; não lê conexão da API.

Exemplo PowerShell usando o laboratório já definido em `compose.inbox.yml`:

```powershell
# Defina uma senha local para o laboratório; não use senha operacional.
$env:INBOX_POSTGRES_PASSWORD = Read-Host 'Senha do PostgreSQL de laboratorio'
docker compose -f compose.inbox.yml up -d --wait inbox-postgres
$env:INBOX_TEST_CONNECTION = "Host=127.0.0.1;Port=55432;Database=inbox_dev;Username=inbox_dev;Password=$env:INBOX_POSTGRES_PASSWORD;Enlist=false"
dotnet build Ordering.Outbox.Benchmarks/Ordering.Outbox.Benchmarks.csproj -c Release
```

Cenário pequeno de correção (200 eventos, 5 bloqueados, 4 warm-ups e 32 amostras totais
por caso, 1 rodada, todos os oito casos; as aquisições avançam pelos três estados elegíveis):

```powershell
dotnet run --project Ordering.Outbox.Benchmarks/Ordering.Outbox.Benchmarks.csproj -c Release --no-build -- --events 200 --blocked 5 --warmup 4 --samples 32 --rounds 1 --output artifacts/outbox-smoke.json
```

Benchmark completo (100.000 eventos, 500 bloqueados, 20 warm-ups e 1.000 amostras totais
por caso/rodada, 3 rodadas):

```powershell
dotnet run --project Ordering.Outbox.Benchmarks/Ordering.Outbox.Benchmarks.csproj -c Release --no-build -- --events 100000 --blocked 500 --warmup 20 --samples 1000 --rounds 3 --output artifacts/outbox-full.json
```

Use um caminho novo para cada execução: arquivos existentes são recusados, preservando
baselines anteriores. `--help` não acessa PostgreSQL. `--events` deve ser múltiplo de 20
(até `int.MaxValue / 7`, para evitar overflow do seed); a distribuição permanece proporcional.
Warm-up e amostras são múltiplos positivos de 4. `--blocked` é positivo e tanto warm-up
quanto amostras devem caber nos elegíveis menos os bloqueados. Essa validação evita
confundir esgotamento da fila com custo de aquisição. `NoEligibleWork`, erros e resultados
incertos ainda são contabilizados, caso ocorram inesperadamente.

`artifacts/` é ignorado pelo Git. Preserve os JSON fora do checkout ou arquive-os com
a revisão usada. O comando pode levar minutos conforme disco, máquina e rede.

## Isolamento e seed

Cada execução cria `outbox_bench_<guid>` a partir de `template0`. Não recebe nomes de
bancos para apagar. Só habilita a remoção depois de confirmar que criou esse nome
único. O banco da conexão administrativa e todos os bancos preexistentes permanecem
intactos. O provisionamento chama `InboxSchema.ApplyAsync`, incluindo as migrações
existentes, sem modificá-las. A fonte Npgsql do banco criado é descartada antes de
`DROP DATABASE ... WITH (FORCE)`, direcionado exclusivamente ao nome criado.

O seed usa `generate_series`, dois `INSERT ... SELECT` e um commit por restauração,
com UUIDs, pedidos confirmados, payloads e ordem determinísticos. Todas as constraints
e referências são aplicadas normalmente. `TRUNCATE` ocorre somente nas duas tabelas
do banco pertencente à execução. Em seguida, `ANALYZE` atualiza ambas e uma consulta
verifica a distribuição efetiva em seis grupos:

- Pending: 10.000 (10%), geração 0.
- RetryScheduled vencido: 5.000 (5%), geração 1.
- RetryScheduled futuro: 15.000 (15%), geração 1.
- Processing expirado: 5.000 (5%), geração 1, owner preenchido.
- Processing ativo: 5.000 (5%), geração 1, owner preenchido.
- Published: 60.000 (60%), geração 1 e data de publicação preenchida.

Um instante do relógio PostgreSQL é fixado por execução. Vencimentos são um dia antes
desse instante e prazos futuros sete dias depois; criação é dois dias antes, desempatada
por microssegundos. Novas aquisições usam lease de uma hora, dentro do contrato real.
Uma fase que dure uma hora é inválida, pois poderia readquirir leases da própria fase.

## Experimentos e fronteiras de medição

Cada rodada executa oito casos: consulta isolada com um worker e chamada real com 1,
2 e 4 workers, todos com zero locks e com a quantidade configurada de locks. Rodadas
pares invertem a ordem dos casos. O total de chamadas é constante entre os casos:
1.000 amostras significam 1.000, 500 ou 250 por worker. Um sinal assíncrono libera os
workers juntos; cada um faz uma chamada por vez, sem retries, pausas ou polling.

Antes do warm-up e novamente antes das amostras, a ferramenta recria a mesma condição
inicial com seed e `ANALYZE`. Reset, comprovação dos locks, EXPLAIN e escrita do relatório
ficam fora das latências e do throughput. Warm-up é guardado separadamente. Os dados
e índices começam novos em cada fase; dentro de uma fase real, os commits alteram a
fila progressivamente. Não há reset entre chamadas da mesma fase.

Na consulta isolada, a latência cobre execução do SQL real e leitura dos seis campos
retornados, em conexão já aberta. `BEGIN`, configurações locais e `ROLLBACK` ficam
fora dessa latência; o throughput de seleções usa o tempo de parede da fase inteira,
incluindo essas transações. Cada seleção é revertida. Isso repete o primeiro candidato
desbloqueado e não representa avanço da fila ou aquisições persistidas.

Na chamada real, a latência inclui todo `TryAcquireAsync`: checkout da conexão,
transação `READ COMMITTED`, configurações de durabilidade/timeouts, seleção, atualização
do lease, commit e descarte. Não há alteração no serviço de produção, Relay ou Kafka.
O pool tem limite 16, sem multiplexação, sem preparo automático e com reset das conexões.
Os limites reais permanecem: lock 2s, statement 5s, idle transaction 10s, comando 10s.

Uma transação auxiliar bloqueia os primeiros candidatos elegíveis, com a mesma
ordenação da consulta real e apenas o limite ampliado. De outra conexão, a ferramenta
comprova **antes e depois** de cada fase: todos os IDs continuam visíveis; nenhum deles
pode ser obtido por `FOR UPDATE SKIP LOCKED`; `FOR UPDATE NOWAIT` no primeiro retorna
SQLSTATE `55P03`; e o PID auxiliar possui lock de transação concedido em `pg_locks`.
Isso verifica todos os candidatos, sem presumir que `pg_locks` liste cada row lock.
Não são 500 transações. O timeout de inatividade da transação auxiliar é desabilitado
somente nela para manter os locks durante o experimento. O `finally` faz rollback e
fecha a conexão, inclusive em falha ou Ctrl+C cooperativo.

O plano vem de `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` concatenado à constante real,
em transação própria com rollback explícito. `EXPLAIN ANALYZE` executa o `FOR UPDATE`;
seu lock é liberado antes das amostras, enquanto os locks auxiliares permanecem.
A ferramenta não muda `enable_seqscan`, custos ou índices nem força outro plano.
O teste funcional existente que força um índice continua sendo apenas teste de
aplicabilidade; o benchmark não reutiliza essa configuração.

## Relatório e validade

O JSON inclui configurações, versão PostgreSQL, runtime/SO/arquitetura do cliente,
SHA Git, indicador de checkout sujo e hashes dos assemblies e do SQL. Nunca contém
connection string, senha ou usuário PostgreSQL. Rodar de outro diretório fora do
repositório falha, pois a identificação do código é obrigatória. Um checkout sujo
requer preservar o patch junto ao relatório para reprodução exata.

Cada experimento registra contagens iniciais/finais, amostras brutas, worker, status,
ID/geração adquiridos e latências p50/p95/p99 por nearest-rank (todas as tentativas e
sucessos separadamente), throughput, aquisições, seleções, `NoEligibleWork`,
`OutcomeUnknown`, erros e timeouts conhecidos. `OutboxStorageUnavailableException`
remove detalhes do provider na implementação real: sua causa pode ser timeout, mas
é relatada como indisponibilidade de causa desconhecida, sem inventar classificação.
Falhas persistidas usam apenas tipo e SQLSTATE, sem mensagens que possam expor segredos.

O plano JSON integral preserva nós, índices, linhas, loops, `Rows Removed ...`, buffers,
tempos de planejamento/execução e demais campos que o servidor disponibilizar. Ausência
de um campo não vira contagem inventada. Não somar contadores de buffers de nós pais e
filhos indiscriminadamente. Linhas examinadas podem ser inferidas dos nós e loops
quando presentes; não há um contador universal exato para todo plano.

`LockComparisons` registra diferenças de p50/p95/p99, tentativas/s e aquisições entre
casos equivalentes da mesma rodada. Não agrega rodadas ocultando sua dispersão. Código
de saída 0 indica fases completas e verificações de consistência aprovadas; 2 indica
experimentos completos com problemas de validade; 1 indica execução incompleta.
Verificações incluem contagem de amostras, estados elegíveis remanescentes, total de
eventos, ausência de IDs duplicados nas aquisições e de IDs bloqueados nos resultados.
Resultados parciais são salvos entre casos, mas nunca constituem baseline completa.

Limitações experimentais:

- Mede cache aquecido/misto. `ANALYZE`, seed, warm-up e EXPLAIN aquecem páginas; não há
  expulsão do cache PostgreSQL ou do sistema operacional. Reset lógico não iguala cache,
  WAL, checkpoints, autovacuum, escalonamento, JIT do cliente ou frequência de CPU.
- Estatísticas do ANALYZE podem variar por amostragem. O plano é coletado no início
  da fase; não prova que o plano permaneceu idêntico após todos os commits.
- Com os parâmetros padrão, as 1.000 aquisições avançam apenas pelos Pending, que têm
  prioridade. Os demais estados compõem a massa/indexação; não são todos adquiridos.
- Um único cliente assíncrono e seu pool representam essa carga específica. Workers
  não simulam hosts independentes nem garantem justiça na distribuição de aquisições.
- Disco, rede, carga concorrente, recursos do contêiner e configurações PostgreSQL
  afetam os números. Execute sem testes concorrentes; registre recursos externos
  do servidor separadamente para comparações entre máquinas.
- O cenário pequeno verifica correção, não sustenta p99 ou conclusões de desempenho.
  Não há asserções rígidas de duração, meta de performance ou alegação de melhoria.
- Ctrl+C permite cleanup cooperativo. Kill abrupto, queda do servidor ou perda da
  confirmação de CREATE/DROP podem deixar o banco único órfão. O nome criado aparece
  no relatório/console; qualquer limpeza posterior deve verificar sua procedência.

## Verificações funcionais

```powershell
dotnet test Ordering.Application.Tests/Ordering.Application.Tests.csproj -c Release --filter FullyQualifiedName~Outbox
dotnet test Ordering.Hosting.Tests/Ordering.Hosting.Tests.csproj -c Release --filter FullyQualifiedName~Outbox
dotnet test Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj -c Release --filter 'FullyQualifiedName~PostgresOutboxProcessingTests|FullyQualifiedName~PostgresInboxTests|FullyQualifiedName~PostgresOrderConfirmationTests'
git diff --check
```

Os testes PostgreSQL também usam bancos únicos via `DatabaseFixture`; precisam de
`INBOX_TEST_CONNECTION`. O benchmark completo é sempre uma operação explícita.

## Medições realizadas neste incremento

Executado sobre o HEAD `5816ff4ab06be400883cf9f8f828e9e9f5a779ee`, com as alterações
locais deste incremento (`Dirty=true`; hashes dos assemblies no JSON). Cliente Windows
x64, runtime .NET 8.0.26, Npgsql 8.0.8; servidor no contêiner `postgres:17.7-bookworm`,
PostgreSQL 17.7, porta local 55439. O contêiner não recebeu limites individuais de
CPU/memória; os recursos da VM Docker e a atividade normal da estação não foram isolados.
A imagem criou um volume anônimo próprio para os dados, removido com o contêiner.

- Compilação Release: aprovada, sem avisos ou erros.
- Testes selecionados: Application 24, Hosting 23, PostgreSQL 97; todos aprovados,
  nenhum ignorado. Não foi reexecutada a integração com Kafka real.
- Smoke com 200 eventos, 5 locks, 4 warm-ups e 32 amostras, 1 rodada: oito casos
  válidos. As aquisições atingiram Pending, RetryScheduled vencido e Processing expirado,
  incluindo gerações 1 e 2. Relatório local: `artifacts/outbox-smoke-mixed.json`.
- Completo com os parâmetros documentados: 24 casos/rodadas válidos, 24.000 medições
  (18.000 aquisições e 6.000 seleções), além de 480 warm-ups. Zero erros, timeouts
  conhecidos, indisponibilidades, resultados incertos ou `NoEligibleWork`.
  Relatório local: `artifacts/outbox-full.json` (planos, amostras e comparações integrais).
- Os 24 planos escolheram `Index Scan` em `ordering_outbox_processing_due`, sem
  forçar o planner. Na primeira rodada, o nó do índice retornou 1 linha sem locks e
  501 com locks; `LockRows` entregou 1. O nó raiz registrou respectivamente 4 e 519
  shared hit blocks. Esses contadores pertencem a uma execução inicial do EXPLAIN,
  não à soma de todas as chamadas reais.
- Exemplo da primeira rodada, consulta isolada: sem locks, p50/p95/p99 de
  0,6200/0,9580/1,2155 ms; com 500 locks, 1,0140/1,5327/2,1581 ms.
  Chamada real com um worker: 3,3271/5,0388/7,5863 ms e 275,73 aquisições/s sem locks;
  4,4706/11,1701/13,6076 ms e 166,63 aquisições/s com locks. As outras rodadas e
  concorrências permanecem no JSON para avaliar a dispersão; esse exemplo não é
  uma estimativa universal nem uma alegação de otimização.
- Comprovados os 500 locks antes/depois de todas as fases correspondentes. Nenhum
  ID bloqueado retornado nem aquisição duplicada. Os bancos de benchmark foram
  removidos; consulta administrativa posterior encontrou somente os bancos padrão
  e o banco administrativo do laboratório, sem tabelas públicas da aplicação.
- Recusa de sobrescrita de relatório existente verificada por SHA-256 preservado.
  `git diff --check` aprovado.

Os relatórios são artefatos locais ignorados pelo Git. Não houve medição de cache frio,
clientes em hosts distintos, carga operacional ou throughput de publicação Kafka.
