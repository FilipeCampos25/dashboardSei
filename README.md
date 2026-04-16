# dashboard_sei

Automacao Selenium para coleta assistida no SEI e dashboard Streamlit para analise da ultima rodada.

## Visao geral

O projeto tem dois blocos conectados:

1. `backend/main.py`
   Executa a coleta no SEI e gera artefatos em `backend/output/` e logs em `output/`.

2. `dashboard_streamlit.py`
   Le os arquivos `*_latest` gerados pelo backend, mostra a ultima rodada em abas separadas por tipo documental e permite disparar uma nova coleta com login manual.

## Setup rapido

1. Instale as dependencias:

```bash
pip install -r requirements.txt
```

2. Copie `.env.example` para `.env` e preencha as credenciais.

## Backend

Execucao padrao:

```bash
python backend/main.py
```

Flags uteis:

- `--debug`
- `--manual-login`
- `--auto-login`
- `--max-internos N`
- `--max-processos N`
- `--no-stop-at-filter`

Exemplo:

```bash
python backend/main.py --manual-login --max-internos 2 --max-processos 3
```

## Artefatos usados pela dashboard

Fontes principais:

- `backend/output/dashboard_ready_latest.csv`
- `backend/output/pt_normalizado_latest.csv`
- `backend/output/pt_auditoria_latest.csv`
- `backend/output/act_normalizado_latest.csv`
- `backend/output/memorando_normalizado_latest.csv`
- `backend/output/ted_normalizado_latest.csv`
- `backend/output/pt_status_execucao_latest.csv`
- `backend/output/act_status_execucao_latest.csv`
- `backend/output/memorando_status_execucao_latest.csv`
- `backend/output/ted_status_execucao_latest.csv`
- `backend/output/performance_analysis.json`
- `output/execution_log_latest.json`

A dashboard trabalha apenas com a ultima rodada. Nao ha historico entre execucoes.

## Dashboard

Execucao:

```bash
streamlit run dashboard_streamlit.py
```

Abas disponiveis:

- `Coleta`
- `Visao Geral`
- `PT`
- `ACT`
- `Memorando`
- `TED`

### O que a dashboard faz

- dispara a coleta pelo botao `Executar coleta`
- encapsula `python backend/main.py --manual-login`
- permite limitar `max_internos` e `max_processos`
- aplica filtros globais por processo, parceiro, qualidade e presenca documental
- separa a analise por tipo documental para nao misturar PT, ACT, Memorando e TED

### Observacao operacional

- o botao da dashboard usa `manual login`
- os dados sao recarregados a partir dos arquivos `*_latest`
- se um arquivo ainda nao existir, a dashboard mostra estado vazio em vez de dados de exemplo

## Variaveis principais de ambiente

- `SEI_URL`
- `SEI_USERNAME`
- `SEI_PASSWORD`
- `HEADLESS`
- `TIMEOUT_SECONDS`
- `MANUAL_LOGIN`
- `MANUAL_LOGIN_WAIT_SECONDS`
- `DEBUG`
- `LOG_LEVEL`
- `OUTPUT_DIR`
- `DESCRICOES_BUSCA`
- `DESCRICOES_MATCH_MODE`
- `DOCUMENT_TYPES`
- `EXPORT_RAW_FIELDS_CSV`
- `TESSERACT_CMD`
- `POPPLER_PATH`

## Dependencias opcionais de extracao

Quando o documento precisa de fallback por arquivo, o backend pode usar:

- `requests`
- `pypdf`
- `pdf2image`
- `pytesseract`

Em Windows, `TESSERACT_CMD` e `POPPLER_PATH` ajudam quando OCR e conversao nao estao no `PATH`.
