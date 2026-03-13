# Desenvolvimento

## Pre-requisitos

- Python 3.11+
- Google Chrome
- Acesso ao SEI

Dependencias opcionais para fallback de anexos:

- Tesseract OCR
- Poppler

## Instalacao

```bash
pip install -r requirements.txt
```

## Configuracao

Use `.env.example` como base.

Variaveis principais:

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

Observacoes:

- `OUTPUT_DIR=output` significa `backend/output/`, porque o backend resolve caminhos relativos a partir da pasta `backend/`.
- `DESCRICOES_BUSCA` define quais internos entram no fluxo guiado.
- `DESCRICOES_MATCH_MODE` aceita `contains` e `equals`. O valor legado `exact` tambem e aceito e convertido para `equals`.
- `REPORT_NAME` existe em `Settings`, mas hoje nao participa do fluxo principal.
- `EXPORT_RAW_FIELDS_CSV=0` desabilita a geracao de `pt_fields_raw.csv`.

## Rodar backend

Padrao:

```bash
python backend/main.py
```

Forcando login manual:

```bash
python backend/main.py --manual-login
```

Tentando login automatico:

```bash
python backend/main.py --auto-login
```

Escopo reduzido para teste:

```bash
python backend/main.py --manual-login --max-internos 1 --max-processos 2
```

Depuracao manual mantendo o filtro aberto:

```bash
python backend/main.py --manual-login --no-stop-at-filter
```

## O que o backend faz hoje

- abre o SEI e valida o pos-login;
- navega em `Bloco > Interno`;
- seleciona internos por descricao;
- gera previa de `PARCERIAS VIGENTES`, quando aplicavel;
- abre cada processo;
- abre o filtro `Pesquisar no Processo`;
- busca `PLANO DE TRABALHO - PT`;
- abre o documento mais recente;
- extrai texto e tabelas do documento;
- persiste snapshots e CSVs tecnicos;
- normaliza os PTs ao final da rodada.

## Artefatos da rodada

Arquivos gerados em `backend/output/`:

- `parcerias_vigentes_latest.csv`
- `plano_trabalho_<processo>.json`
- `pt_fields_raw.csv`
- `pt_status_execucao_latest.csv`
- `pt_sem_prazo_latest.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`

Comportamento importante:

- esses arquivos sao limpos no inicio de cada rodada;
- `pt_fields_raw.csv` e append durante a rodada, mas comeca do zero porque o arquivo anterior e removido na preparacao;
- `pt_normalizado_completo_latest.csv` contem apenas os registros classificados como `completo_padronizado`.

## Extracao de documento

O extrator tenta primeiro o DOM HTML do `iframe` de visualizacao.

Fallbacks implementados:

- espera adicional por renderizacao;
- download do anexo pelo link do documento;
- leitura nativa de PDF;
- OCR de PDF com `pdf2image` + `pytesseract`;
- leitura de DOCX quando o anexo vem empacotado.

Se o ambiente Windows nao tiver OCR/conversao no `PATH`, configure:

- `TESSERACT_CMD`
- `POPPLER_PATH`

## Normalizacao de PT

O normalizador:

1. le `plano_trabalho_*.json`;
2. tenta enriquecer com `parcerias_vigentes_latest.csv`;
3. extrai parceiro, objeto, periodo, atribuicoes, metas e acoes;
4. classifica cada linha em:
   - `completo_padronizado`
   - `parcial_padronizado`
   - `extraido_sem_padrao`

## Dashboard

Execucao:

```bash
streamlit run dashboard_streamlit.py
```

Entrada esperada:

- `output/sei_dashboard.csv`

Se o arquivo nao existir:

- o app mostra warning;
- usa dados de exemplo embutidos.

## Gap atual de integracao

O backend nao grava automaticamente `output/sei_dashboard.csv`.

Hoje o estado correto do projeto e:

- backend gera dados tecnicos e normalizados em `backend/output/`;
- dashboard consome um CSV canonico na raiz;
- falta uma etapa de transformacao entre esses dois contratos.
