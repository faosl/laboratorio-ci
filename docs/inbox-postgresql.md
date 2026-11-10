# Inbox PostgreSQL — primeiro incremento

A porta `IInboxStore` pertence a Application; PostgreSQL/Npgsql pertencem exclusivamente
ao projeto de infraestrutura. O domínio, os contratos existentes, o adaptador Uber e o
endpoint não foram alterados. A API não referencia a infraestrutura nem provisiona o banco.
O chamador da porta é responsável por autenticar a notificação antes do registro.

## Executar localmente

Requisitos: SDK .NET 8 ou compatível, runtime .NET 8, Docker com containers Linux.
Defina uma senha exclusivamente local; não a versione. PowerShell:

```powershell
$env:INBOX_POSTGRES_PASSWORD = [Guid]::NewGuid().ToString('N')
docker compose -f compose.inbox.yml up -d --wait
$env:INBOX_TEST_CONNECTION = "Host=localhost;Port=55432;Database=inbox_dev;Username=inbox_dev;Enlist=false;Password=$env:INBOX_POSTGRES_PASSWORD"
dotnet run --project Ordering.Inbox.TestHost -c Release -- migrate
dotnet test Ordering.Infrastructure.PostgreSql.Tests -c Release
```

O volume nomeado preserva dados ao recriar o container. Não execute `down -v` para
validar recuperação: isso elimina o armazenamento. Reinicializações posteriores precisam
usar a senha originalmente definida no volume; mudar a variável não troca a senha do banco.
O comando `docker compose -f compose.inbox.yml stop` encerra o banco sem apagar dados.
O bind da porta é somente em localhost. Esta configuração não é infraestrutura de produção.
A imagem PostgreSQL tem versão explícita; futuras atualizações devem passar pela suíte.

A fixture exige PostgreSQL real e falha se a variável estiver ausente. Cria um banco
com nome aleatório por execução, aplica o esquema duas vezes e o remove ao terminar.
A credencial de teste precisa de CREATEDB e propriedade dos bancos criados; não usar
esse privilégio para a aplicação operacional. A CI usa credencial sintética descartável.

## Modelo e equivalência

- `uuid`: identificador interno gerado pelo cliente, sem reutilizar o id do parceiro.
- `varchar(200) COLLATE "C"`: identidade exata, sensível a maiúsculas, sem trim ou
  conversão automática. O limite evita chaves arbitrariamente grandes nos índices.
- `bytea`: preservação exata do corpo, de 1 byte até 1 MiB. Não é JSON reserializado.
- `jsonb`: representação normalizada de `OrderNotification`, com EventId e OrderId.
  Uma restrição confere sua igualdade com os identificadores tipados.
- `integer`: versão normalizada; somente versão 1 é aceita neste incremento.
- `timestamptz`: recebimento definido pelo banco e recuperado em UTC; precisão de microssegundos.
- `state`: inicialmente e exclusivamente Pending. Novos estados exigirão nova migração.

A chave UNIQUE obrigatória é `(partner_id, integration_scope, event_id)`. `order_id`
não é único: eventos diferentes podem referenciar o mesmo pedido. O índice adicional
`(state, received_at_utc, inbox_id)` prepara consultas de pendências e ordenação estável;
não implementa reservas, workers ou processamento. Não se indexa o payload completo.

Duplicata equivalente nesta versão significa a mesma chave, o mesmo OrderId, a mesma
versão, a mesma projeção normalizada (igualdade jsonb) e bytes originais idênticos.
Uma diferença de espaços no JSON original já resulta em Conflict. Não existe
equivalência semântica específica da Uber. Recebimento, id interno e estado não entram
na comparação e não são atualizados na reentrega. O envelope copia os buffers recebidos
e devolvidos para evitar mutações acidentais.

## Transação, durabilidade e resultados

`RegisterAsync` usa READ COMMITTED, INSERT ON CONFLICT DO NOTHING e uma leitura separada
do vencedor. A restrição resolve concorrência entre processos; a leitura posterior
evita a armadilha de um snapshot do mesmo statement não enxergar o vencedor.
A leitura do existente usa FOR SHARE para impedir alteração/exclusão até o fim da transação.
Registro completo e metadados são inseridos juntos. O conteúdo existente não é atualizado.

Cada registro impõe SET LOCAL synchronous_commit=on e verifica primário, fsync=on e
full_page_writes=on. O esquema usa tabela permanente com WAL, nunca UNLOGGED.
O armazenamento deve honrar flush; a aplicação não consegue verificar isso por SQL.
Não há replicação, failover nem tolerância à perda do volume neste incremento.

- Inserted: commit confirmado com persistência síncrona local.
- AlreadyExists: conteúdo equivalente encontrado sem alteração.
- Conflict: mesma chave com conteúdo incompatível, sem sobrescrita.
- OutcomeUnknown: falha de transporte, timeout ou cancelamento durante a fase de commit;
  nenhum id é apresentado como confirmado. Isso não significa rollback.

Falhas anteriores ao commit são propagadas; a transação é descartada sem commit.
Falhas de leitura também são propagadas: erro não é ausência. Não há retry oculto.
O chamador pode consultar GetByKey e repetir Register com a mesma chave.
Ausência em uma leitura não prova rollback: o commit pode ainda estar em andamento.

Visibilidade não é, sozinha, evidência de flush. Tanto GetByKey quanto Register leem
o existente com FOR SHARE e concluem a transação com synchronous_commit=on. O bloqueio
da tupla atribui um transaction id e gera WAL; confirmar essa transação força também
o WAL anterior, incluindo o commit da inserção observada. A consulta, portanto, exige
primário gravável e tem custo de WAL e bloqueio, sem modificar os campos do registro.
Falha ao confirmar a transação de leitura é propagada sem devolver evidência de sucesso.
O protocolo é exclusivamente local: não constitui confirmação em réplica.

## Esquema e operação

`Migrations/001_inbox.sql` é incorporado ao assembly. `InboxSchema.ApplyAsync` é uma
operação explícita de provisionamento: aplica a migração e registra sua versão na mesma
transação, sob advisory lock. Não editar migrações já aplicadas; acrescentar novas versões
e evolução do executor em incrementos futuros. Não há provisionamento automático na API.

Não são armazenados tokens, segredos ou assinaturas. A biblioteca não registra payloads,
connection strings ou erros em logs. O consumidor não deve registrar exceções/parameters
indiscriminadamente. Produção exigirá TLS, criptografia dos volumes/backups e papel com
SELECT/INSERT e a permissão UPDATE exigida por FOR SHARE, separado do papel de migração.
UPDATE/DELETE de dados não fazem parte desta porta. Credenciais, ambiente e escopo estável
devem ser configurados pelo host.

Não há expurgo automático. A política futura deverá definir horizontes para payload e
identidade separadamente, proteger pendências e considerar cópias em backups. Excluir a
identidade termina a garantia de deduplicação para reentregas antigas. Retenção indefinida
do payload não é uma política de produção aprovada.

## Evidências e limites dos testes

Os testes reais cobrem round-trip completo, bytes arbitrários, versão, UTC, Pending,
reentrega, conflito de corpo/pedido, 24 conexões/fontes independentes concorrentes,
isolamento por parceiro/escopo, eventos distintos do mesmo pedido e rollback pré-commit.
Dois processos dotnet distintos gravam e recuperam o mesmo envelope, sem compartilhar memória.

Os cenários de commit indeterminado executam COMMIT ou ROLLBACK reais e injetam perda de
resposta na fronteira interna de teste. Outro cenário mantém a transação pendente e
comprova que uma leitura ausente pode preceder um commit posterior. Esses testes
demonstram classificação e reconciliação; não simulam fisicamente perda de pacotes,
falha de energia ou quebra do protocolo Npgsql. Os hooks são internos, indisponíveis
na porta Application e no construtor público.

Configurações efetivas e persistência da tabela são conferidas. Encerrar os processos
cliente demonstra recuperação sem memória local; não demonstra recuperação do servidor
após crash de sistema operacional. Perda de disco, restauração de backup, flush físico,
partições de rede e failover exigem testes representativos futuros.

O endpoint continua retornando 503. R1 não está demonstrado de ponta a ponta; não há
ACK operacional, processamento, POS, Outbox, Kafka ou efeitos externos idempotentes.

## Resultado da execução local

Em 10/11/2026, com nova compilação Release para net8.0 e PostgreSQL 17.7 real:

- Infraestrutura PostgreSQL: 14 aprovados.
- Application: 5 aprovados, incluindo 4 novos casos de validação/imutabilidade do envelope.
- Domain: 7 aprovados.
- Contracts.Partners: 16 aprovados.
- Integrations.UberEats: 120 aprovados.
- API: 38 aprovados, mantendo o comportamento HTTP de laboratório.

Total final: 200 aprovados, 0 reprovados, 0 ignorados. Falhas intermediárias na primeira
implementação da barreira de WAL foram corrigidas antes dessa execução final.
O workflow foi atualizado, mas não foi executado remotamente nesta entrega.
Não houve alteração fora do escopo aprovado nem mudança de política de publicação.
