---
name: proposal-analysis
description: Analise propostas de novas implementações ou refatorações em modo somente leitura, compare alternativas relevantes e recomende uma solução antes de codificar.
---

# Análise de proposta técnica

## Quando usar
Use quando o usuário solicitar análise, investigação de alternativas, desenho de uma solução ou revisão de uma proposta **antes da implementação**.

Não use para executar implementações já aprovadas, revisar um diff após a implementação ou investigar incidentes operacionais sem pedido de proposta.

## Limite de autorização
- **Somente leitura.** Não crie, edite, remova ou formate arquivos; não altere código, dependências, configurações, branches, commits, pipelines ou documentação.
- Não execute comandos que modifiquem o workspace, incluindo instalações, geração de código, builds e testes que escrevam artefatos, sem autorização explícita adicional.
- Não trate o pedido de análise como autorização para implementar. Encerre apresentando a proposta e os critérios de verificação.
- Leia o AGENTS.md aplicável e respeite suas convenções e restrições. Instruções específicas da tarefa delimitam o escopo; sinalize conflitos ou impossibilidades.

## Procedimento
1. **Compreenda o comportamento pedido.** Resuma objetivo, entradas, saídas, regras de negócio, invariantes, requisitos não funcionais e exclusões expressas pelo usuário. Não presuma requisitos não informados.
2. **Investigue o repositório existente.** Em leitura, localize os componentes e contratos relevantes, dependências, testes, comandos de CI e padrões já adotados. Separe fatos observados, hipóteses e recomendações.
3. **Identifique a fronteira da mudança.** Determine quem deve possuir a responsabilidade e quais contratos, camadas e dependências seriam impactados. Preserve decisões arquiteturais já aprovadas; não proponha novas tecnologias sem motivo verificável.
4. **Compare abordagens somente quando houver escolha material.** Normalmente considere de uma a três alternativas. Discuta trade-offs pertinentes: correção, coesão, acoplamento, segurança, custo de manutenção, testabilidade, desempenho, operação e complexidade. Se uma opção for claramente suficiente, evite alternativas artificiais.
5. **Recomende uma abordagem.** Justifique por requisitos e evidências. Explique quais componentes ou arquivos provavelmente seriam envolvidos, sem gerar alterações. Diferencie decisões obrigatórias, escolhas delegáveis e questões ainda abertas.
6. **Defina critérios de aceitação verificáveis.** Inclua cenários positivos, negativos e limites relevantes; identifique testes unitários, de contrato ou de integração conforme a responsabilidade. Especifique o que cada teste demonstraria e o que permaneceria sem comprovação.
7. **Controle o escopo.** Registre riscos, pressupostos e trabalhos futuros relevantes, mas não antecipe a implementação de funcionalidades não solicitadas.

## Forma da resposta
Seja objetivo e use estes blocos quando fizerem sentido:
- **Contexto verificado:** estado atual e referências a arquivos realmente examinados.
- **Alternativas e trade-offs:** apenas diferenças que alterem a decisão.
- **Recomendação:** abordagem e justificativa técnica.
- **Verificação proposta:** comportamentos e evidências esperadas.
- **Limites e pendências:** exclusões, pressupostos e riscos.

Explique termos técnicos novos ao utilizá-los. Não confunda sucesso de teste existente com comprovação de código ainda não implementado. Não alegue que compilou, testou ou modificou algo que não executou. Quando houver informação ausente não bloqueante, assuma explicitamente o cenário mínimo em vez de ampliar o trabalho.
