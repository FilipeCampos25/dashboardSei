# dashboard_sei

Automacao Selenium para coleta assistida no SEI + dashboard Streamlit para analise local de CSV.

## Visao geral

O sistema tem dois blocos independentes:

1. `backend/main.py`
Executa a navegacao no SEI, entra nos internos filtrados, abre processos, localiza o Plano de Trabalho mais recente e gera artefatos em `backend/output/` por padrao.

2. `dashboard_streamlit.py`
Le um CSV canonico em `output/sei_dashboard.csv` na raiz do repositorio. Se esse arquivo nao existir, sobe com dados de exemplo.

Hoje nao existe integracao automatica entre esses dois blocos. O backend gera artefatos tecnicos e analiticos em `backend/output/`; o dashboard consome outro contrato de dados.

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

- `--debug`: forca log em `DEBUG`
- `--manual-login`: espera login manual
- `--auto-login`: tenta login com credenciais
- `--max-internos N`
- `--max-processos N`
- `--no-stop-at-filter`: abre o filtro do processo e mantem a aba aberta para depuracao manual

Exemplo:

```bash
python backend/main.py --debug --manual-login --max-internos 2 --max-processos 3
```

## Fluxo real do backend

1. Carrega `.env`, logs e Chrome WebDriver.
2. Abre o SEI e confirma o pos-login.
3. Fecha pop-up inicial, se existir.
4. Navega em `Bloco > Interno`.
5. Lista internos e filtra por `DESCRICOES_BUSCA`.
6. Para cada interno selecionado:
   - reabre a lista na pagina correta;
   - entra no interno;
   - se a descricao for `PARCERIAS VIGENTES`, gera `parcerias_vigentes_latest.csv`;
   - lista os processos;
   - abre cada processo;
   - abre todas as pastas;
   - aciona `Pesquisar no Processo`;
   - busca `PLANO DE TRABALHO - PT`;
   - abre o documento mais recente;
   - extrai snapshot textual/tabelas do documento;
   - salva JSON, CSV raw e relatorios de acompanhamento.
7. Ao fim da rodada, normaliza os JSONs de PT e gera CSVs consolidados.

## Artefatos gerados pelo backend

Por padrao em `backend/output/`:

- `parcerias_vigentes_latest.csv`
- `plano_trabalho_<processo>.json`
- `pt_fields_raw.csv`
- `pt_status_execucao_latest.csv`
- `pt_sem_prazo_latest.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`

Observacao importante:

- Cada execucao limpa esses artefatos `*_latest` e os `plano_trabalho_*.json` anteriores antes de iniciar uma nova rodada.

## Dashboard

Execucao:

```bash
streamlit run dashboard_streamlit.py
```

Entrada esperada:

- `output/sei_dashboard.csv`

Se o arquivo nao existir, o app mostra um warning e usa dados de exemplo.

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
- `EXPORT_RAW_FIELDS_CSV`
- `TESSERACT_CMD`
- `POPPLER_PATH`

## Dependencias opcionais de extracao

O extrator tenta ler o documento diretamente do DOM. Quando o PT esta como anexo/PDF, o fallback usa:

- `requests`
- `pypdf` ou `PyPDF2`
- `pdf2image`
- `pytesseract`

`TESSERACT_CMD` e `POPPLER_PATH` ajudam em ambientes Windows onde OCR/conversao nao estao no `PATH`.

## VS Code debug

Use a configuracao `Backend SEI (Debug)` em `.vscode/launch.json`.
