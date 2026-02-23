# Arquitetura

## Visao geral
O projeto possui dois blocos principais:

1. Backend de automacao (Selenium) para navegar no SEI e coletar dados/documentos.
2. Dashboard Streamlit para analise de registros estruturados em CSV.

O entrypoint `backend/main.py` ja executa o fluxo assistido completo (`SEIScraper.run_full_flow`), com login manual ou automatico, selecao guiada de internos por descricao, coleta direcional de "PARCERIAS VIGENTES" e varredura de documentos dos processos.

## Modulos principais
- `backend/main.py`: CLI do backend (`--manual-login`, `--auto-login`, limites de internos/processos) e ciclo de vida do driver.
- `backend/app/config.py`: carrega `.env` via `dotenv` + `pydantic-settings`.
- `backend/app/core/driver_factory.py`: cria Chrome WebDriver (Selenium Manager por padrao, `CHROMEDRIVER_PATH` opcional).
- `backend/app/core/logging_config.py`: configura logging global e reduz ruido de libs de terceiros.
- `backend/app/services/selectors.py`: carrega `backend/app/rpa/xpath_selector.json`.
- `backend/app/services/reporting.py`: exporta colecoes para CSV/XLSX com pandas.
- `backend/app/rpa/scraping.py`: fluxo principal de navegacao/coleta no SEI (`SEIScraper`).
- `dashboard_streamlit.py`: dashboard que le `output/sei_dashboard.csv` (raiz do repositorio) e gera KPIs/graficos/tabela.

## Fluxo real do backend (`SEIScraper.run_full_flow`)
1. Carrega URL/credenciais/config e abre o SEI.
2. Executa login manual (padrao) ou tenta login automatico pelos seletores de `xpath_selector.json`.
3. Fecha pop-up inicial (se existir).
4. Navega no menu `Bloco > Interno`.
5. Lista internos com paginacao e extrai `numero_interno` + `descricao`.
6. Filtra internos por `DESCRICOES_BUSCA` (match `contains` ou `equals`).
7. Para cada interno selecionado:
   - reabre a lista e navega ate a pagina correta;
   - clica no interno;
   - se a descricao do interno for exatamente `PARCERIAS VIGENTES`, executa coleta preview estruturada e salva CSV;
   - lista processos do interno;
   - abre cada processo em nova aba/janela;
   - expande pastas da arvore de documentos (`ifrArvore`) e coleta nomes dos documentos;
   - fecha aba do processo e retorna.
8. Retorna a lista unica de documentos encontrados (`self.found`).

## Coleta de dados (implementado hoje)
### 1) Coleta direcional de "PARCERIAS VIGENTES"
Quando o interno atual tem descricao `PARCERIAS VIGENTES`, o scraper:

- percorre a tabela `tblProtocolosBlocos` com paginacao;
- identifica a linha do processo;
- faz parse da coluna de anotacoes;
- extrai campos estruturados:
  - `processo`
  - `parceiro`
  - `vigencia`
  - `objeto`
  - `numero_act` (ACT)
  - `seq`
- salva em `backend/output/parcerias_vigentes_YYYYMMDD_HHMMSS.csv` (por padrao).

### 2) Coleta de documentos por processo
Para cada processo do interno selecionado, o scraper abre a arvore de documentos e coleta os nomes dos documentos. Isso permite localizar/registrar itens como:

- Memorando de Entendimento
- TED
- ACT
- Plano de Trabalho

Os nomes coletados ficam no conjunto `self.found` (saida em memoria + log).

## Destaque de escopo de coletagem (negocio)
O fluxo foi estruturado para suportar a coletagem de:

- `parcerias vigentes`
- `Memorando de Entendimento`
- `TED`
- `ACT`
- `Plano de Trabalho` (incluindo desdobramento em metas, acoes e prazos no dataset analitico)

Destaque solicitado (termo de negocio):
- Coletagem de "parcerias vigentes" / Memorando de Entendimento (`processo`, `documento`, `parceiro`, `vigencia`, `objeto`, `atribuicoes`, Plano de Trabalho -> `metas`, `acoes` e `prazos`), incluindo `TED` e `ACT`.

Estado atual:
- Ja implementado de forma estruturada no backend: preview de `PARCERIAS VIGENTES` (`processo`, `parceiro`, `vigencia`, `objeto`, `numero_act`).
- Ja implementado como varredura de documentos: nomes dos documentos dos processos (onde aparecem Memorando/TED/ACT/Plano de Trabalho).
- Esperado no dashboard (contrato): `documento`, `atribuicao`, `meta`, `acao`, `prazo` etc., dependendo da etapa de estruturacao da coleta.

## Dashboard e contrato de dados
O dashboard trabalha com colunas canonicas:

- `processo`
- `documento`
- `parceiro`
- `vigencia_inicio`
- `vigencia_fim`
- `objeto`
- `atribuicao`
- `meta`
- `acao`
- `prazo`
- `status`
- `fonte`
- `collected_at`

Ele aceita aliases de colunas, tenta extracao textual a partir de `linha` e calcula `vigencia_status`.

## Observacoes arquiteturais importantes
- O backend gera hoje preview em `backend/output/parcerias_vigentes_*.csv` (relativo ao backend).
- O dashboard le `output/sei_dashboard.csv` na raiz do repositorio.
- Portanto, ainda existe um gap de integracao/orquestracao entre a coleta atual e o arquivo consumido pelo dashboard.
