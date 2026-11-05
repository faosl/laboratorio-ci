# Ordering API — Orientações para agentes

## Arquitetura
- Preserve a separação entre Domain, Application,
  Contracts e API.
- Não introduza dependências de infraestrutura
  no modelo de domínio.
- Mantenha contratos normalizados independentes
  dos formatos específicos dos parceiros.

## Implementação
- Utilize .NET 8 e siga as convenções existentes.
- Não amplie o escopo sem justificativa e aprovação.
- Não altere políticas de publicação sem autorização.

## Verificação
- Execute os testes relevantes.
- Apresente os arquivos alterados e os resultados.
- Diferencie testes aprovados de comportamentos
  que ainda não foram verificados.