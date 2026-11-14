# Webhook HTTP com Inbox no laboratório

A coordenação persistente de processamento foi acrescentada separadamente, preservando
este recebimento: [leases e fencing tokens](inbox-processing-leases.md). Não há worker executável.

O host `Ordering.API.LaboratoryHost` executa o fluxo completo em .NET 8, reutilizando
o endpoint da API. A API continua sem referência a PostgreSQL e sem provisionamento
automático. Sua execução padrão, sem Inbox configurada, retorna 503 para notificações válidas.

## Execução local

Use Docker Linux e o procedimento de provisionamento de [inbox-postgresql.md](inbox-postgresql.md).
Com `INBOX_TEST_CONNECTION` apontando para o banco local já migrado:

```powershell
$env:ASPNETCORE_ENVIRONMENT = 'Laboratory'
$env:Inbox__ConnectionString = $env:INBOX_TEST_CONNECTION
$env:UberEats__IntegrationScope = 'laboratory-integration-01'
# Segredo sintético exclusivo deste laboratório; forneça o mesmo ao cliente de teste.
$env:UberEats__SharedSecret = [Guid]::NewGuid().ToString('N')
dotnet run --project Ordering.API.LaboratoryHost -c Release --no-launch-profile
```

As três configurações são obrigatórias. Ausência, segredo em branco ou escopo inválido
impedem a inicialização, com mensagem genérica. Conexão indisponível ou esquema não
provisionado nunca autoriza confirmação. O host não executa migrações.
O endereço padrão é `http://127.0.0.1:5088`; não configure exposição pública.
O host recusa ambientes diferentes de Laboratory e o mapeamento compartilhado
também verifica o ambiente. Não use credenciais nem notificações reais.

Para enviar uma notificação sintética em outra sessão, com o mesmo segredo configurado:

```powershell
$body = [Text.Encoding]::UTF8.GetBytes('{"event_type":"orders.notification","event_id":"event-1","meta":{"resource_id":"order-1"}}')
$key = [Text.Encoding]::UTF8.GetBytes($env:UberEats__SharedSecret)
$hmac = [Security.Cryptography.HMACSHA256]::new($key)
try { $signature = ([BitConverter]::ToString($hmac.ComputeHash($body))).Replace('-', '').ToLowerInvariant() }
finally { $hmac.Dispose() }
Invoke-WebRequest -Uri 'http://127.0.0.1:5088/webhooks/uber-eats/orders' -Method Post -ContentType 'application/json' -Headers @{ 'X-Uber-Signature' = $signature } -Body $body
```

Não versione os valores, não registre corpos/assinaturas/segredos e remova as variáveis
sensíveis da sessão após o exercício. O papel de provisionamento é separado do futuro
papel operacional; o exemplo local não estabelece uma configuração de produção.

## Sequência e responsabilidades

1. O endpoint valida transporte e tamanho, preservando até 1 MiB de bytes originais.
2. `UberEatsOrderNotificationOrchestrator` autentica esses bytes antes de decodificar
   UTF-8, desserializar e mapear. Entrada rejeitada não alcança a persistência.
3. `RegisterAuthenticatedOrderNotificationUseCase` recebe o contrato normalizado e
   os mesmos bytes autenticados. Autenticação é sua precondição; ele não conhece Uber,
   HTTP, assinaturas, segredos ou Npgsql.
4. O caso de uso compõe a chave e o envelope híbrido versão 1 e aguarda `IInboxStore`.
5. Somente Inserted/AlreadyExists com ID confirmado geram 200 com corpo vazio.
   Conflict gera 409, OutcomeUnknown gera 503, indisponibilidade transitória
   identificada gera 503 e erro inesperado gera 500. Respostas são genéricas.

400/403/413/415/422 continuam identificando os erros de entrada existentes. Limites
de identificadores são validados explicitamente; falhas internas não são convertidas
indiscriminadamente em 422. A identidade de integração é configuração confiável.

`PartnerId` é sempre `uber-eats`. `IntegrationScope` identifica uma integração estável:
preserve-o em reinícios, múltiplas instâncias e rotação de segredo. Use escopos distintos
para integrações/ambientes independentes. Não derive o escopo de segredo, processo,
timestamp ou cabeçalho. Nenhum identificador de evento/pedido é aparado ou convertido
para outra caixa. A definição do namespace real da conta depende de validação futura
antes de uso operacional.

## Deduplicação e falhas

A chave é `(PartnerId, IntegrationScope, EventId)`, protegida por UNIQUE no PostgreSQL.
Mesmo pedido pode ter vários eventos. Equivalência exige também mesmo OrderId,
versão, representação normalizada e bytes idênticos. Alterar espaços do JSON pode
gerar Conflict; o registro original, recebimento e estado não são sobrescritos.

OutcomeUnknown não significa rollback. Não há consulta ou retry automático dentro
da requisição. Uma reentrega autenticada com a mesma chave/corpo pode reconhecer o
registro confirmado ou inserir após rollback. Ausência numa leitura não prova que
um commit pendente falhou. Falha da barreira de durabilidade de uma duplicata também
impede 200. Se o cliente desconectar depois do commit, uma reentrega continua segura.

200 confirma armazenamento, não processamento, aceitação pelo restaurante ou envio ao POS.

## Verificação

```powershell
dotnet test Ordering.Application.Tests -c Release
dotnet test Ordering.API.Tests -c Release
dotnet test Ordering.Infrastructure.PostgreSql.Tests -c Release
```

A última suíte exige `INBOX_TEST_CONNECTION`, cria bancos isolados e não ignora testes
na ausência do PostgreSQL. Inclui HTTP real via TestServer, orquestrador e armazenamento
PostgreSQL reais, 24 hosts/fontes independentes, inspeção por conexão independente,
bloqueio antes do commit, falha pré-commit, commit/rollback reais com confirmação
perdida, falha da confirmação de duplicata, conflitos e isolamento. As fronteiras de
falha são internas ao assembly de testes já autorizado; não são contratos públicos.
O host executável também é exercitado com sua composição efetiva.

A CI executa esses testes no job existente com PostgreSQL 17.7. Nenhuma política de
publicação, atestação ou ativação operacional é modificada.

## Limites

O armazenamento mantém `synchronous_commit=on`, exige primário com fsync e
full_page_writes habilitados e usa tabela permanente com WAL. Duplicatas passam pela
barreira de WAL existente. Isso sustenta a durabilidade local sob a premissa de que
o armazenamento honra flush. Testes com perda de resposta injetada verificam a
classificação e a reentrega; não simulam fisicamente perda de pacotes ou energia.

Não foram implementados replicação, failover, tolerância à perda do volume, workers,
Outbox, Kafka, consultas de pedidos ou POS. Recuperação após crash do servidor,
flush físico, restauração e promoção de réplica exigem infraestrutura e ensaios
operacionais futuros. O endpoint permanece não autorizado para tráfego real do parceiro.

## Evidências desta implementação — 11/11/2026

Execução local após `dotnet clean` em Release, com compilação para net8.0 e
PostgreSQL 17.7 real em container isolado, exposto somente em localhost:

- Domain: 7 aprovados.
- Contracts.Partners: 16 aprovados.
- Integrations.UberEats: 120 aprovados.
- Application: 9 aprovados.
- API: 38 aprovados, incluindo os testes HTTP anteriores sem Inbox.
- PostgreSQL/HTTP durável: 33 aprovados, incluindo os 14 casos anteriores da Inbox.

Total final: **223 aprovados, 0 reprovados, 0 ignorados**. Os testes novos acrescentam
4 casos de Application e 19 de composição/HTTP/PostgreSQL. Erros intermediários nos
novos testes (assinatura sintética, comparação textual e sintaxe) foram corrigidos
antes das execuções finais. `git diff --check` não apontou erros.

O teste que suspende a transação antes do commit observa a requisição incompleta e
a ausência do registro por outra conexão, liberando 200 somente após a confirmação.
Os testes de commit/rollback com confirmação perdida obtêm 503 antes de reentregar;
a reentrega confirma o registro existente ou realiza a inserção após rollback.
Uma falha da transação que confirma a durabilidade de duplicata também obtém 503.

Essas evidências são locais. A CI foi integrada pelo projeto de testes já executado
no job com PostgreSQL; o workflow remoto não foi executado nesta sessão. Não foram
testados crash físico, perda de energia/volume, replicação ou failover.
Nenhuma política de publicação ou funcionalidade fora do escopo foi alterada.

Arquivos criados:

- `Ordering.Application/Inbox/RegisterAuthenticatedOrderNotificationUseCase.cs`
- `Ordering.Application/Inbox/InboxStorageUnavailableException.cs`
- `Ordering.API/UberEatsIntegrationIdentity.cs`
- `Ordering.API/UberEatsServices.cs`
- `Ordering.API.LaboratoryHost/Ordering.API.LaboratoryHost.csproj`
- `Ordering.API.LaboratoryHost/LaboratoryProgram.cs`
- `Ordering.Application.Tests/RegisterAuthenticatedOrderNotificationTests.cs`
- `Ordering.Infrastructure.PostgreSql.Tests/UberEatsInboxHttpTests.cs`
- `docs/uber-eats-http-inbox.md`

Arquivos alterados:

- `Ordering.API/Program.cs`
- `Ordering.API/UberEatsOrderEndpoint.cs`
- `Ordering.Application/Inbox/IInboxStore.cs`
- `Ordering.Infrastructure.PostgreSql/PostgresInboxStore.cs`
- `Ordering.Infrastructure.PostgreSql.Tests/Ordering.Infrastructure.PostgreSql.Tests.csproj`
- `.github/workflows/ordering-api.yml` (nome da etapa que já executa o projeto ampliado)
- `README.md`
- `docs/inbox-postgresql.md`
