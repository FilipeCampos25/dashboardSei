# Desenvolvimento

## Pre-requisitos
- Python 3.11+ (recomendado 3.12).
- Google Chrome instalado.
- Acesso ao ambiente SEI com credenciais validas.

## Instalacao
1. Criar e ativar ambiente virtual.
2. Instalar dependencias:

```bash
pip install -r requiriments.txt
```

## Configuracao
Crie/ajuste o arquivo `.env` com:

```env
url_sei=https://sei.defesa.gov.br/
username=<seu_usuario>
password=<sua_senha>
HEADLESS=true
TIMEOUT_SECONDS=20
OUTPUT_DIR=output
REPORT_NAME=sei_dashboard
LOG_LEVEL=INFO
```

Observacoes:
- `HEADLESS=true` executa sem abrir janela do navegador.
- `LOG_LEVEL` aceito: `DEBUG`, `INFO`, `WARNING`, `ERROR`.
- `OUTPUT_DIR` e `REPORT_NAME` ja estao previstos em `Settings`, mas ainda nao sao usados pelo `main.py` atual para gravacao.

## Execucao
### Backend (login SEI)
```bash
python backend/main.py
```

Comportamento atual:
- Faz login no SEI.
- Tenta fechar popup inicial.
- Encerra o navegador.

### Dashboard
```bash
streamlit run dashboard_streamlit.py
```

Comportamento:
- Le `output/sei_dashboard.csv` se existir.
- Caso contrario, usa dados de exemplo internos.

## Estrutura relevante
- `backend/main.py`: entrypoint do backend.
- `backend/app/config.py`: variaveis de ambiente.
- `backend/app/rpa/scraping.py`: fluxo de scraping.
- `backend/app/rpa/xpath_selector.json`: seletores da automacao.
- `backend/app/services/reporting.py`: exportacao CSV/XLSX.
- `dashboard_streamlit.py`: aplicacao Streamlit.

## Estado atual e proximos ajustes tecnicos
- Para coleta real, incluir `coleta.linhas_tabela` em `backend/app/rpa/xpath_selector.json`.
- Para gerar arquivo consumido pelo dashboard, integrar `SEIScraper.run()` + `ReportBuilder.to_csv()` no `backend/main.py`.
- Recomenda-se criar pasta `output/` antes da primeira gravacao.

## Troubleshooting rapido
- Erro de login: validar `url_sei`, `username` e `password`.
- Elemento nao encontrado: revisar XPaths no `xpath_selector.json`.
- Dashboard sem dados reais: confirmar existencia de `output/sei_dashboard.csv`.
- ChromeDriver falhando: atualizar Chrome local e reinstalar dependencias.
