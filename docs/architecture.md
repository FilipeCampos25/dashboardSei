# Arquitetura

## Visao geral
O projeto esta dividido em dois fluxos independentes:

1. Coleta (RPA Selenium) no backend.
2. Visualizacao analitica (Streamlit + Plotly) no dashboard.

Hoje, o entrypoint `backend/main.py` executa apenas login no SEI (`run_login_only`). O fluxo completo de navegacao + coleta existe em `SEIScraper.run()`, mas ainda nao esta conectado ao `main.py`.

## Estrutura de modulos
- `backend/main.py`: inicializa configuracao, logging e ChromeDriver; executa login e encerra navegador.
- `backend/app/config.py`: carrega variaveis de ambiente com `dotenv` e expoe `Settings` imutavel.
- `backend/app/rpa/core/driver_factory.py`: cria o WebDriver Chrome (headless opcional).
- `backend/app/rpa/core/logging_config.py`: padroniza formato e nivel de log.
- `backend/app/rpa/scraping.py`: implementa `SEIScraper` (login, fechamento de popup, navegacao e coleta).
- `backend/app/rpa/xpath_selector.json`: mapa central de seletores XPath.
- `backend/app/services/reporting.py`: utilitario para exportar registros para CSV/XLSX.
- `dashboard_streamlit.py`: carrega `output/sei_dashboard.csv`, normaliza colunas e renderiza filtros/KPIs/graficos/tabela.

## Fluxo backend (estado atual)
1. `get_settings()` le `.env`.
2. `setup_logging()` configura logs.
3. `create_chrome_driver()` sobe o navegador.
4. `SEIScraper.run_login_only()` executa login + tentativa de fechar popup.
5. Driver e encerrado com `quit()`.

## Fluxo backend (implementado, mas nao ligado ao entrypoint)
`SEIScraper.run()` executa:
1. Login.
2. Fechamento de popup inicial (quando presente).
3. Navegacao para bloco interno.
4. Coleta de linhas via XPath `coleta.linhas_tabela`.

Se `coleta.linhas_tabela` nao estiver mapeado, o scraper retorna apenas metadados da sessao (`status=sem_linhas_mapeadas`).

## Fluxo dashboard
1. Tenta ler `output/sei_dashboard.csv`.
2. Se nao existir, usa dataset de exemplo interno.
3. Normaliza schema com aliases para colunas canonicas.
4. Tenta extrair campos textuais quando existe coluna `linha`.
5. Converte datas e calcula `vigencia_status`.
6. Aplica filtros laterais e renderiza:
- KPIs.
- Graficos (barra, pizza, linha, barra por status).
- Tabela detalhada.
- Download de modelo CSV.

## Contrato de dados esperado no dashboard
Colunas canonicas:
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

O dashboard aceita aliases e preenche colunas faltantes com valor nulo.

## Limitacoes conhecidas
- `backend/main.py` ainda nao persiste dados em `output/sei_dashboard.csv`.
- `ReportBuilder` existe, mas nao e usado no entrypoint atual.
- `xpath_selector.json` nao possui o bloco `coleta.linhas_tabela`; sem isso, nao ha captura de linhas reais.
