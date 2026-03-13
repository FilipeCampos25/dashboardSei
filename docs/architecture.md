# Arquitetura

## Visao geral

O repositorio esta organizado em dois executaveis desacoplados:

1. Backend Selenium
Responsavel por autenticar no SEI, navegar nos internos, abrir processos, localizar o Plano de Trabalho e persistir artefatos tecnicos em disco.

2. Dashboard Streamlit
Responsavel por ler um CSV canonico local e exibir filtros, KPIs, graficos e tabela analitica.

O backend hoje produz dados em `backend/output/`. O dashboard hoje consome `output/sei_dashboard.csv` na raiz. Esse desacoplamento ainda nao foi resolvido por pipeline interno.

## Entrypoints

- `backend/main.py`
CLI principal do backend. Resolve flags, configura logging e chama `SEIScraper.run_full_flow(...)`.

- `dashboard_streamlit.py`
Aplicacao Streamlit que tenta carregar `output/sei_dashboard.csv`.

## Modulos principais do backend

- `backend/app/config.py`
Carrega `.env` com `dotenv` e expoe `Settings` via `pydantic-settings`.

- `backend/app/core/driver_factory.py`
Cria o Chrome WebDriver. Usa Selenium Manager por padrao e `CHROMEDRIVER_PATH` como fallback.

- `backend/app/core/logging_config.py`
Configura logging global.

- `backend/app/rpa/selectors.py`
Carrega e valida `backend/app/rpa/xpath_selector.json`.

- `backend/app/rpa/scraping.py`
Orquestrador principal do fluxo SEI.

- `backend/app/rpa/sei/process_navigation.py`
Abre processos e gerencia troca de abas/janelas.

- `backend/app/rpa/sei/toolbar_actions.py`
Opera a toolbar do processo ate abrir o filtro `Pesquisar no Processo`.

- `backend/app/rpa/sei/document_search.py`
Seleciona o tipo exato no filtro e abre o resultado mais recente.

- `backend/app/rpa/sei/document_text_extractor.py`
Extrai texto e tabelas do documento aberto. Faz fallback para download/PDF/OCR quando necessario.

- `backend/app/core/raw_date_field_collector.py`
Extrai campos brutos relacionados a data e periodo a partir do snapshot do documento.

- `backend/app/services/pt_normalizer.py`
Cruza JSONs de PT com a previa de `PARCERIAS VIGENTES` e gera CSVs normalizados.

- `backend/app/output/csv_writer.py`
Escrita padronizada de CSV.

## Fluxo real do backend

### 1. Preparacao da rodada

Antes de iniciar a navegacao, `SEIScraper._prepare_output_dir_for_run()` limpa os artefatos anteriores no diretório de saida:

- `plano_trabalho_*.json`
- `pt_fields_raw.csv`
- `pt_status_execucao_latest.csv`
- `pt_sem_prazo_latest.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`
- `parcerias_vigentes_latest.csv`

### 2. Entrada no SEI

`run_full_flow()`:

1. valida `SEI_URL`;
2. abre a URL;
3. confirma login manual ou tenta login automatico;
4. registra a janela principal;
5. fecha pop-up inicial, se houver.

### 3. Navegacao de internos

O scraper:

1. abre `Bloco > Interno`;
2. pagina a listagem;
3. coleta `numero_interno` e `descricao`;
4. filtra pelos termos de `DESCRICOES_BUSCA`, em modo `contains` ou `equals`;
5. retorna aos internos selecionados e entra um a um.

### 4. Coleta de previa de `PARCERIAS VIGENTES`

Se o interno atual tiver descricao equivalente a `PARCERIAS VIGENTES`, o scraper percorre `tblProtocolosBlocos`, pagina a grade e gera:

- `interno_descricao`
- `seq`
- `processo`
- `parceiro`
- `vigencia`
- `numero_act`
- `objeto`

Saida:

- `backend/output/parcerias_vigentes_latest.csv`

### 5. Navegacao por processo

Para cada processo do interno:

1. abre o processo em nova aba/janela;
2. aguarda pagina pronta;
3. aciona `Abrir todas as Pastas`;
4. abre `Pesquisar no Processo`;
5. busca o tipo exato `PLANO DE TRABALHO - PT`;
6. tenta abrir o documento mais recente;
7. se a busca falhar, usa fallback pela arvore do processo.

### 6. Extracao do Plano de Trabalho

Quando o documento esta aberto, o sistema:

1. entra no `iframe` de visualizacao;
2. extrai `body.innerText` e tabelas HTML;
3. se o conteudo estiver vazio ou intermediario, aguarda renderizacao;
4. se ainda assim falhar, tenta localizar o link de download;
5. se o anexo for PDF, tenta extracao nativa e depois OCR;
6. monta um snapshot com:
   - `text`
   - `tables`
   - `url`
   - `title`
   - `extraction_mode`

### 7. Persistencia de artefatos

Para cada PT encontrado:

- salva `plano_trabalho_<processo>.json`
- atualiza `pt_fields_raw.csv`
- adiciona linha a um relatorio interno de status de execucao

Ao final da rodada:

- grava `pt_status_execucao_latest.csv`
- grava `pt_sem_prazo_latest.csv`
- roda `export_normalized_csv(...)`
- grava `pt_normalizado_latest.csv`
- grava `pt_normalizado_completo_latest.csv`

## Contratos de dados atuais

### Contrato produzido pelo backend

Arquivos em `backend/output/`:

- `parcerias_vigentes_latest.csv`
Previa estruturada por processo.

- `plano_trabalho_<processo>.json`
Snapshot bruto do documento PT.

- `pt_fields_raw.csv`
Modelo long com campos brutos e evidencias textuais.

- `pt_status_execucao_latest.csv`
Status de execucao da rodada.

- `pt_sem_prazo_latest.csv`
Subset dos PTs sem periodo completo detectado.

- `pt_normalizado_latest.csv`
Normalizacao consolidada dos snapshots de PT.

- `pt_normalizado_completo_latest.csv`
Subset classificado como `completo_padronizado`.

### Contrato esperado pelo dashboard

Arquivo na raiz:

- `output/sei_dashboard.csv`

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

## Dashboard

O dashboard:

1. tenta ler `output/sei_dashboard.csv`;
2. aplica aliases de colunas;
3. tenta completar campos a partir de uma coluna `linha`;
4. faz parse de datas;
5. calcula `vigencia_status`;
6. renderiza filtros, KPIs, graficos e tabela.

Se o CSV nao existir, usa um dataset de exemplo embutido.

## Lacuna arquitetural atual

Ainda falta uma etapa que converta os artefatos do backend, especialmente `pt_normalizado_latest.csv`, para o contrato `output/sei_dashboard.csv`.
