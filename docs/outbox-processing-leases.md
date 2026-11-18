# Transactional Outbox — incremento 2: coordenação persistente

Implementado na `main`, a partir de `80ed7f85a24c7e6e73b8997e2afb8e01914d49a4`.
Este incremento coordena mensagens no PostgreSQL. Não inclui Kafka, Publisher executável,
polling, novos endpoints, processamento POS ou alterações em políticas de publicação.

## Contrato e composição

`Ordering.Application.Outbox.IOutboxProcessingStore` oferece `TryAcquireAsync`, `RenewAsync`,
`MarkPublishedAsync` e `ScheduleRetryAsync`, sem dependências de Npgsql. O adaptador
`PostgresOutboxProcessingStore` recebe explicitamente um `NpgsqlDataSource`; nenhum host
passa a processar a Outbox automaticamente. Os contratos e a operação da Inbox permanecem
inalterados, assim como a confirmação atômica do pedido criada no incremento 1.

`OutboxLease` contém `EventId`, `Owner` e `Generation`. O owner identifica uma execução
de worker e admite até 200 caracteres, sem NUL ou valor em branco. As mesmas convenções
do laboratório são usadas em tipos próprios da Outbox: lease entre 1 segundo e 1 hora;
retry entre zero e 7 dias; ambos com precisão de microssegundos. Isso evita vincular a
semântica da Outbox à Inbox ou introduzir uma refatoração transversal neste incremento.

`AcquiredOutboxMessage` carrega o lease, a expiração e os dados persistidos do evento:
`EventId`, `EventType`, `AggregateId`, `Payload` e `CreatedAtUtc`. O payload é a representação
textual do JSONB persistido, sem conversão para um envelope de parceiro ou broker.

## Migração e invariantes

A migração aditiva `004_outbox_processing.sql` é um recurso embutido aplicado pelo
provisionamento versionado `InboxSchema.ApplyAsync`, sob o advisory lock existente.
O comando explícito `Ordering.Inbox.TestHost migrate` também a aplica. A API não executa
migrações no startup. Os scripts 001, 002 e 003 não foram alterados.

Os eventos existentes continuam `Pending`, com geração zero e os novos campos nulos.
Identificadores, conteúdo, data de criação, dados da Inbox e histórico anterior são
preservados. Reaplicar o provisionamento preserva também as datas de todas as versões.

As constraints impõem:

- `Pending`: geração zero; sem owner, expiração, agendamento ou data de publicação.
- `Processing`: geração positiva, owner não vazio e expiração finita; sem agendamento
  ou data de publicação.
- `RetryScheduled`: geração positiva e próxima tentativa finita; sem owner, expiração
  ou data de publicação.
- `Published`: geração positiva e data de publicação finita; sem owner, expiração
  ou próxima tentativa.

As operações do protocolo nunca escrevem nos cinco campos imutáveis do evento.
`Published` é terminal para o protocolo; alterações administrativas diretas não estão
protegidas por esse contrato. Não há trigger que impeça um administrador de editar dados.

## Aquisição e autoridade

Cada operação usa uma transação curta `READ COMMITTED`, força `synchronous_commit = on`
e requer um primary com `fsync` e `full_page_writes` habilitados. Os limites locais são
2 segundos para espera por lock, 5 segundos por statement, 10 segundos de inatividade
na transação e 10 segundos de timeout no comando cliente.

A aquisição faz work claiming com `SELECT ... FOR UPDATE SKIP LOCKED`. São elegíveis
mensagens pendentes, retries vencidos e leases expirados. O índice parcial
`ordering_outbox_processing_due` acompanha a expressão e a ordenação da consulta.
Pendentes têm instante lógico `-infinity` nessa expressão para serem imediatamente
elegíveis, independentemente da data de criação; retries e recuperação usam seu prazo
persistido. A data de criação e o UUID desempatam candidatos. Isso prioriza pendentes e não
oferece FIFO absoluto, justiça entre estados ou ausência de starvation sob carga contínua.

Após bloquear a linha, uma nova instrução usa `clock_timestamp()` materializado para
revalidar a elegibilidade, incrementar a geração, registrar owner/expiração e limpar o
agendamento anterior. Em `long.MaxValue`, a aquisição falha sem alterar dados: nunca há
overflow ou reutilização de fencing token. Esse esgotamento requer intervenção e pode
impedir progresso enquanto a linha continuar sendo o primeiro candidato.

A reserva só é retornada após confirmação do commit e liberação dos locks/transação.
`NoEligibleWork` pode indicar linhas bloqueadas por outras transações; não prova uma
Outbox vazia. `SKIP LOCKED` evita espera por row locks de candidatos, mas não elimina
possíveis esperas por locks de tabela.

Renovação, conclusão e retry primeiro obtêm o lock da linha e então validam, em outra
instrução, identidade, estado `Processing`, owner, geração e expiração ainda válida.
O instante fresco do banco é obtido **depois** de qualquer espera por lock. A validação
e a escrita ficam protegidas pelo mesmo lock; nenhum sucessor pode intervir entre elas.
Um lease expirado é rejeitado mesmo sem sucessor. A recuperação incrementa a geração,
invalidando o worker anterior mesmo se o owner for reutilizado.

A autorização vale no instante atômico da validação no banco, não no momento posterior
do commit ou da resposta ao cliente. Renovar não reduz a expiração vigente. Esses prazos
usam o relógio civil do PostgreSQL; não representam uma garantia de tempo monotônico.

`ScheduleRetryAsync` calcula o próximo prazo no banco e libera a reserva atomicamente;
delay zero permite nova aquisição imediata. `MarkPublishedAsync` registra apenas a
conclusão local, preenche `published_at_utc` e libera owner/expiração. No futuro, seu
chamador deverá obter confirmação do broker antes dessa operação. Não existe confirmação
fictícia de broker nem comprovação de entrega externa aqui.

## Falhas e resultados

- Aquisição: `Acquired`, `NoEligibleWork` ou `OutcomeUnknown`.
- Mutação: `Succeeded`, `LeaseLost` ou `OutcomeUnknown`.

Falhas anteriores à fronteira de commit propagam e permitem rollback por descarte da
transação. Falhas transitórias do provider são traduzidas para
`OutboxStorageUnavailableException`. Na fronteira de commit, falha de comunicação,
erro Npgsql, timeout ou cancelamento resulta em `OutcomeUnknown`: a transação pode ter
feito commit ou rollback, e o resultado não distingue esses casos.

Aquisição indeterminada retorna `Work = null`; renovação indeterminada não fornece
expiração nem comprova continuidade da autoridade. Não há retries automáticos ocultos.
Fencing protege as escritas locais deste protocolo e não impede duplicação de efeitos
em sistemas externos. Nenhuma conexão ou transação é mantida para uma futura chamada ao Kafka.

## Verificação local — 18/11/2026

Reutilizados `DatabaseFixture` e `INBOX_TEST_CONNECTION`: PostgreSQL real 17.7 em container
descartável, restrito a `127.0.0.1:55432`, com as configurações de durabilidade acima.
A fixture cria e remove bancos isolados; a conexão precisa de permissão `CREATEDB`.
A ausência da variável causa falha explícita, sem ignorar testes. Consulte a
[configuração PostgreSQL do laboratório](inbox-postgresql.md).

Projetos compilados para .NET 8 com SDK 10.0.101 e runtime .NET 8.0.26. Comandos executados:

```powershell
dotnet test Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj -c Release --filter FullyQualifiedName~PostgresOutboxProcessingTests --logger 'console;verbosity=minimal'
dotnet test Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj -c Release --logger 'console;verbosity=minimal'
dotnet test Ordering.Application.Tests/Ordering.Application.Tests.csproj -c Release --logger 'console;verbosity=minimal'
dotnet test Ordering.Hosting.Tests/Ordering.Hosting.Tests.csproj -c Release --logger 'console;verbosity=minimal'
dotnet test Ordering.Dominio.Tests/Ordering.Dominio.Tests.csproj -c Release --logger 'console;verbosity=minimal'
dotnet test Ordering.Contracts.Partners.Tests/Ordering.Contracts.Partners.Tests.csproj -c Release --logger 'console;verbosity=minimal'
dotnet test Ordering.Integrations.UberEats.Tests/Ordering.Integrations.UberEats.Tests.csproj -c Release --logger 'console;verbosity=minimal'
dotnet test Ordering.API.Tests/Ordering.API.Tests.csproj -c Release --logger 'console;verbosity=minimal'
git diff --check
```

Resultados finais:

- PostgreSQL: **160 aprovados**, incluindo **63 novos casos de coordenação da Outbox**.
- Application: **77 aprovados**, incluindo **6 novos casos do contrato Outbox**.
- Hosting: **51 aprovados**.
- Domain: **7 aprovados**.
- Contracts: **16 aprovados**.
- Integração Uber Eats: **120 aprovados**.
- API: **38 aprovados**.

Total: **469 aprovados, zero falhas, zero ignorados**. Compilação sem avisos ou erros.
`git diff --check` passou. A primeira execução foi bloqueada pelo acesso ao NuGet no sandbox;
as execuções finais usaram acesso autorizado. Erros intermediários na declaração dos dados
de teste e na limpeza de transações do teste foram corrigidos antes da execução final.

Os testes verificam upgrade real 003→004 com preservação de eventos, Inbox e histórico,
provisionamento repetível, constraints, dados imutáveis, aquisição confirmada e liberação
de locks, concorrência entre instâncias, recuperação, renovação, fencing, retry e estado
terminal. Validam também aplicabilidade do índice e esgotamento de geração.

A concorrência usa barreiras `TaskCompletionSource` e evidência do próprio PostgreSQL:
`FOR UPDATE NOWAIT` comprova que o primeiro candidato está bloqueado enquanto outro
worker adquire um evento distinto; `pg_stat_activity` e `pg_blocking_pids` comprovam a
espera por um bloqueador conhecido nos testes de horário fresco e cancelamento.
O intervalo de polling limita a frequência de observação, sem servir como prova do lock.

As falhas são injetadas em fronteiras internas, sem alterar o contrato Application.
Os testes comprovam rollback antes do commit e classificam respostas perdidas após
commit real e rollback real para aquisição e todas as mutações. São exercitados IO,
timeout, cancelamento e erro Npgsql, sem expor lease ou expiração utilizável no resultado
indeterminado e sem repetição automática da operação.

Não foram verificados perda física de pacotes, queda de energia, crash/restart do servidor,
honestidade do armazenamento, replicação, failover ou desempenho e justiça sob carga de
produção. Não foram comprovadas publicação Kafka, entrega at-least-once efetiva,
exactly-once ou segurança de produção. Os resultados acima correspondem à execução
local; o estado da CI remota deve ser consultado no commit enviado ao repositório.
