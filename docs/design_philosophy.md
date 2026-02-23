# Filosofia de Design

## Objetivo do projeto
Automatizar a navegacao no SEI para apoiar a coleta de dados de parcerias e documentos correlatos, com uma trilha de evolucao para analise operacional em dashboard.

O desenho atual prioriza:

- robustez de navegacao (iframes, timeouts, paginação, pop-up);
- coleta incremental (preview estruturado + varredura de documentos);
- documentacao clara do que ja esta implementado vs. o que ainda e etapa de integracao.

## Principios adotados
- Separacao de responsabilidades: config, driver, logging, seletores, scraping e reporting em modulos separados.
- Tolerancia a variacoes de UI: o scraper tenta seletores alternativos e varre iframes quando necessario.
- Coleta guiada por descricao: os internos sao filtrados por `DESCRICOES_BUSCA`, reduzindo escopo e ruido.
- Incrementalismo pragmatico: primeiro garantir acesso + navegacao + preview de "PARCERIAS VIGENTES", depois expandir estruturacao completa.
- Observabilidade: logs detalhados em pontos criticos (timeouts, iframes, paginação, documentos encontrados).

## Decisoes praticas atuais
- Login manual e o padrao (`MANUAL_LOGIN=true`), com opcao de login automatico via CLI/env.
- Selenium Manager e usado por padrao para resolver o ChromeDriver; `CHROMEDRIVER_PATH` e fallback.
- `xpath_selector.json` centraliza os seletores do fluxo SEI.
- `ReportBuilder` e usado na exportacao do preview de `PARCERIAS VIGENTES` para CSV.
- O dashboard continua desacoplado do backend em tempo real e consome CSV local.

## Escopo de coletagem destacado (negocio)
O projeto foi direcionado para a coletagem de:

- parcerias vigentes
- Memorando de Entendimento
- TED
- ACT
- Plano de Trabalho (metas, acoes e prazos)

Cobertura atual no codigo:
- Estruturado: preview de `PARCERIAS VIGENTES` com `processo`, `parceiro`, `vigencia`, `objeto` e `numero_act`.
- Semi-estruturado: nomes de documentos coletados por processo (onde aparecem Memorando/TED/ACT/Plano de Trabalho).
- Analitico (dashboard): schema preparado para `documento`, `atribuicao`, `meta`, `acao`, `prazo`, `status`, `fonte`.

## Trade-offs assumidos
- XPaths absolutos aceleram manutencao inicial, mas aumentam sensibilidade a mudancas do SEI.
- Login manual como padrao reduz falhas de autenticacao complexa, mas exige intervencao do operador.
- Preview de `PARCERIAS VIGENTES` gera valor rapido, mas nao substitui ainda o pipeline canonico `output/sei_dashboard.csv`.
- Parser textual de anotacoes e heuristico; pode exigir ajustes conforme variacao de preenchimento.

## Diretrizes de evolucao
- Transformar a varredura de documentos em extracao estruturada de Memorando/TED/ACT/Plano de Trabalho.
- Popular `output/sei_dashboard.csv` na raiz com o schema canonico do dashboard.
- Padronizar campos de vigencia (`vigencia_inicio` / `vigencia_fim`) a partir do texto de `vigencia`.
- Adicionar testes para parsing de anotacoes (`parceiro`, `vigencia`, `objeto`, `numero_act`) e normalizacao de textos.
- Revisar e endurecer seletores para reduzir fragilidade a mudancas de layout do SEI.
