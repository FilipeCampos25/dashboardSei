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
- procura `pt`, `act`, `memorando` e `ted`;
- tenta aliases no filtro antes de cair para a arvore;
- extrai texto e tabelas do documento;
- classifica semanticamente o snapshot;
- persiste bronze, silver e gold;
- normaliza PT e a familia de cooperacao ao final da rodada.

## Artefatos da rodada

Arquivos gerados em `backend/output/`:

- `parcerias_vigentes_latest.csv`
- `plano_trabalho_<processo>.json`
- `acordo_cooperacao_tecnica_<processo>.json`
- `pt_fields_raw.csv`
- `pt_auditoria_latest.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`
- `act_status_execucao_latest.csv`
- `act_normalizado_latest.csv`
- `memorando_status_execucao_latest.csv`
- `memorando_normalizado_latest.csv`
- `ted_status_execucao_latest.csv`

Comportamento importante:

- esses arquivos `latest` sao limpos no inicio de cada rodada;
- os JSONs brutos sao a camada bronze;
- os arquivos `*_status_execucao_latest.csv` e `pt_auditoria_latest.csv` sao a camada silver;
- os arquivos `*_normalizado_latest.csv` sao a camada gold;
- `pt_normalizado_completo_latest.csv` contem apenas os registros classificados como `completo_padronizado`.

## Extracao de documento

O extrator tenta primeiro o DOM HTML do `iframe` de visualizacao.

Fallbacks implementados:

- espera adicional por renderizacao;
- download do anexo pelo link do documento;
- leitura nativa de PDF;
- OCR de PDF com `pdf2image` + `pytesseract`;
- leitura de DOCX quando o anexo vem empacotado em `zip_docx`.

Se o ambiente Windows nao tiver OCR/conversao no `PATH`, configure:

- `TESSERACT_CMD`
- `POPPLER_PATH`

## Classificacao e publicacao

Familias de cooperacao:

- usam `classify_cooperation_snapshot(...)`;
- aceitam alias de filtro por tipo documental;
- publicam gold apenas quando `publication_status=published_gold`;
- retêm em silver minutas, extratos, termos aditivos, documentos relacionados e `not_found`.

PT:

- publica em gold apenas candidatos canonicos;
- rebaixa `documentacao/minutas` para silver;
- registra `validation_status`, `publication_status` e `classification_reason`;
- registra `period_source`, inclusive `direct_label`, `derived_from_signature`, `unresolved_relative` e `unresolved_noise`.

## Normalizacao de PT

O normalizador:

1. le `plano_trabalho_*.json`;
2. tenta enriquecer com `parcerias_vigentes_latest.csv`;
3. extrai parceiro, objeto, periodo, atribuicoes, metas e acoes;
4. reconhece prazo textual ou relativo;
5. pode derivar vigencia a partir da assinatura quando o documento disser `a partir da assinatura` ou equivalente;
6. classifica cada linha em:
   - `completo_padronizado`
   - `parcial_padronizado`
   - `extraido_sem_padrao`

## Testes

Executar o conjunto mais relevante para parser e familias documentais:

```bash
python -m unittest tests.test_document_types tests.test_act_handler tests.test_act_normalizer tests.test_pt_normalizer
```

Para focar so no PT:

```bash
python -m unittest tests.test_pt_normalizer
```

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

O backend ainda nao grava automaticamente `output/sei_dashboard.csv`.

Hoje o estado correto do projeto e:

- backend gera bronze, silver e gold em `backend/output/`;
- dashboard consome um CSV canonico na raiz;
- falta uma etapa de publicacao entre esses dois contratos.
