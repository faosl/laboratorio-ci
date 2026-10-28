# Ordering.API — laboratório local

## GitHub Actions

`.github/workflows/ordering-api.yml` executa `test` e `docker` em Pull Requests
e pushes na `main`. Não há suíte de testes de domínio: o job `test` compila uma
vez e executa testes HTTP de `/health` e `/` na saída publicada. O job `docker`
usa `Dockerfile.ci` para empacotar esses mesmos binários sem recompilar e repete
os testes na imagem final. O Dockerfile original mantém o build local completo.

Após o smoke test, a imagem é salva com `docker save` e transferida como artefato.
`publish` valida o SHA-256 do arquivo e o ID da imagem antes de carregar, retaggear
e enviar a mesma imagem ao GHCR; não executa build. Os artefatos duram um dia.

A publicação está **desativada por padrão**. Somente após autorização explícita,
um administrador deve configurar a variável de repositório `ENABLE_IMAGE_PUBLISH`
como `true`. O job só poderá então executar em um próximo push na `main`, após
o sucesso de `docker`. Não há gatilho manual nem publicação em Pull Requests.
O destino é `ghcr.io/<owner>/<repo>/ordering-api:sha-<commit>` e somente `publish`
recebe `packages: write`. As configurações do repositório/organização devem
permitir publicação no GHCR usando `GITHUB_TOKEN`.

Os IDs garantem a identidade da imagem carregada, não equivalência entre o ID
local e o digest de manifesto do registry, que representam objetos diferentes.
As tags das actions e imagens base não estão fixadas por digest/SHA; isso limita
a reprodutibilidade. A suíte HTTP não substitui futuros testes de regras de pedidos.

API mínima ASP.NET Core em .NET 8, sem dependências externas.
`GET /health` retorna HTTP 200 e `Healthy` quando o processo está saudável.
Esse health check verifica a aplicação, sem testar banco de dados ou outros serviços.

Execute a partir da raiz, com Docker Engine Linux disponível:

```powershell
docker build -t ordering-api:local .
docker run --detach --name ordering-api-lab --publish 127.0.0.1:8080:8080 ordering-api:local
curl.exe --include --fail http://localhost:8080/health
docker logs ordering-api-lab
docker stop ordering-api-lab
docker rm ordering-api-lab
```

O build usa o SDK para compilar e publicar em Release; a imagem final contém
somente o runtime ASP.NET e a aplicação publicada, executada pelo usuário `app`.
O arquivo de projeto é copiado antes do código para aproveitar o cache de restore.
`EXPOSE` documenta a porta; `--publish` torna essa porta acessível apenas no host local.
O Dockerfile usa tags de versão 8.0 para receber atualizações; para releases
reprodutíveis, registre e fixe os digests das imagens base validadas.
O build baixa as imagens base e pode consultar o NuGet; não publica a imagem.
