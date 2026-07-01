# Arquitetura

## Visao geral

O repositorio continua organizado em dois executaveis desacoplados:

1. Backend Selenium
Responsavel por autenticar no SEI, navegar nos internos, abrir processos, localizar documentos de parceria e persistir artefatos tecnicos em disco.

2. Dashboard Streamlit
Responsavel por ler os artefatos `latest`, consolidar uma carteira gerencial em memoria e exibir filtros, KPIs, tabelas e qualidade da base.

O backend produz dados em `backend/output/`. O dashboard consome diretamente `backend/output/*_latest` e `output/execution_log_latest.json`, sem criar ou exigir `output/sei_dashboard.csv`.

## Entrypoints

- `backend/main.py`
CLI principal do backend. Resolve flags, configura logging e chama `SEIScraper.run_full_flow(...)`.

- `dashboard_streamlit.py`
Aplicacao Streamlit que carrega a ultima rodada e delega consolidacao, metricas, qualidade e renderizacao aos servicos de dashboard.

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
Lista resultados do filtro e ajuda a abrir candidatos por alias e recencia.

- `backend/app/rpa/sei/document_text_extractor.py`
Extrai texto e tabelas do documento aberto. Faz fallback para download, PDF, OCR e DOCX quando necessario.

- `backend/app/documents/types.py`
Define `DocumentTypeSpec`, inclusive `accepted_doc_classes` e `filter_type_aliases`.

- `backend/app/documents/pt.py`
Define a familia documental de PT, seus artefatos e a persistencia de tracking.

- `backend/app/documents/cooperation_common.py`
Implementa o handler compartilhado das familias de cooperacao, inclusive silver e gold.

- `backend/app/documents/act.py`
- `backend/app/documents/memorando.py`
- `backend/app/documents/ted.py`
Registram as especificacoes de cada familia documental.

- `backend/app/services/act_normalizer.py`
Classifica snapshots da familia de cooperacao com `classify_cooperation_snapshot(...)` e exporta a gold de ACT.

- `backend/app/core/raw_date_field_collector.py`
Extrai campos brutos relacionados a data e periodo a partir do snapshot do documento.

- `backend/app/services/pt_normalizer.py`
Cruza JSONs de PT com a previa de `PARCERIAS VIGENTES`, endurece o parse de periodo e gera silver e gold de PT.

- `backend/app/output/csv_writer.py`
Escrita padronizada de CSV.

- `backend/app/services/dashboard_data.py`
Leitura segura dos arquivos `latest`, metadados de coleta e compatibilidade com arquivos ausentes, vazios ou malformados.

- `backend/app/services/dashboard_metrics.py`
Regras puras de vigencia, cobertura, moeda e resumo de texto.

- `backend/app/services/dashboard_portfolio.py`
Modelo canonico de carteira, deduplicacao, classificacao ativo/historico e enriquecimento por PT, TED e memorando.

- `backend/app/services/dashboard_quality.py`
Cobertura, conflitos, duplicidades, fila de revisao e criterios para bloquear graficos sobre dimensoes sujas.

- `backend/app/services/dashboard_views.py`
Componentes Streamlit das paginas gerenciais.

## Fluxo real do backend

### 1. Preparacao da rodada

Antes de iniciar a navegacao, `SEIScraper._prepare_output_dir_for_run()` limpa os artefatos `latest` anteriores no diretorio de saida.

Entre os arquivos reciclados pela rodada estao:

- `plano_trabalho_*.json`
- `acordo_cooperacao_tecnica_*.json`
- `memorando_*.json`
- `pt_fields_raw.csv`
- `pt_auditoria_latest.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`
- `act_status_execucao_latest.csv`
- `act_normalizado_latest.csv`
- `memorando_status_execucao_latest.csv`
- `memorando_normalizado_latest.csv`
- `ted_status_execucao_latest.csv`
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
5. itera as familias documentais configuradas;
6. para cada familia, tenta primeiro os aliases de filtro;
7. consolida candidatos semanticamente validos ou, se necessario, usa fallback pela arvore;
8. registra na silver os casos `not_found`, `extraction_failure`, `minuta` ou `related_but_not_canonical`.

### 6. Extracao e classificacao de documento

Quando o documento esta aberto, o sistema:

1. entra no `iframe` de visualizacao;
2. extrai `body.innerText` e tabelas HTML;
3. se o conteudo estiver vazio ou intermediario, aguarda renderizacao;
4. se ainda assim falhar, tenta localizar o link de download;
5. se o anexo for PDF, tenta extracao nativa e depois OCR;
6. se o anexo for DOCX ou `zip_docx`, faz leitura estruturada;
7. monta um snapshot com:
   - `text`
   - `tables`
   - `url`
   - `title`
   - `extraction_mode`
8. envia o snapshot para classificacao semantica.

### 7. Classificacao semantica

Familias de cooperacao usam:

- `classify_cooperation_snapshot(snapshot, requested_type, collection_context=None)`

Retornos principais:

- `doc_class`
- `resolved_document_type`
- `is_canonical_candidate`
- `validation_status`
- `publication_status`
- `discard_reason`
- `classification_reason`

Semantica atual:

- `published_gold` apenas para o tipo canonicamente valido para a familia pedida;
- `retained_silver` para minutas, extratos, termos aditivos, documentos relacionados e `not_found`.

PT usa analise equivalente, mas com regras especificas de periodo, assinatura e canonicidade de minuta/documentacao.

### 8. Persistencia de artefatos

Bronze:

- snapshots JSON por processo e familia

Silver:

- `pt_auditoria_latest.csv`
- `act_status_execucao_latest.csv`
- `memorando_status_execucao_latest.csv`
- `ted_status_execucao_latest.csv`

Gold:

- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`
- `act_normalizado_latest.csv`
- `memorando_normalizado_latest.csv`

## Contratos de dados atuais

### Contrato produzido pelo backend

Arquivos em `backend/output/`:

- `parcerias_vigentes_latest.csv`
Previa estruturada por processo.

- `plano_trabalho_<processo>.json`
Snapshot bruto do documento PT.

- `acordo_cooperacao_tecnica_<processo>.json`
Snapshot bruto do candidato ou documento da familia ACT.

- `pt_fields_raw.csv`
Modelo long com campos brutos e evidencias textuais.

- `pt_auditoria_latest.csv`
Auditoria silver de PT, inclusive `validation_status`, `publication_status` e `period_source`.

- `pt_normalizado_latest.csv`
Gold de PT, com apenas registros publicados.

- `pt_normalizado_completo_latest.csv`
Subset dos PTs classificados como `completo_padronizado`.

- `act_status_execucao_latest.csv`
Silver da familia ACT.

- `act_normalizado_latest.csv`
Gold da familia ACT.

- `memorando_status_execucao_latest.csv`
Silver da familia memorando.

- `memorando_normalizado_latest.csv`
Gold da familia memorando.

- `ted_status_execucao_latest.csv`
Silver da familia TED.

- `ted_normalizado_latest.csv`
Gold normalizada de TED quando disponivel, incluindo valor, vigencia e unidades.

- `parcerias_descontinuadas_normalizado_latest.csv`
Historico de parcerias encerradas, nao realizadas ou com status inconsistente.

- `dashboard_ready_latest.csv`
Base consolidada inicial por processo/instrumento, com melhores campos conhecidos e qualidade.

- `divergence_matrix_latest.csv`
Matriz de divergencias e origem dos melhores campos usados na carteira.

- `normalization_review_queue_latest.csv`
Fila de revisao e qualidade.

### Contrato consumido pelo dashboard

Fontes primarias:

- `backend/output/dashboard_ready_latest.csv`
- `backend/output/parcerias_vigentes_latest.csv`
- `backend/output/parcerias_descontinuadas_normalizado_latest.csv`
- `backend/output/pt_auditoria_latest.csv`
- `backend/output/pt_normalizado_latest.csv`
- `backend/output/act_normalizado_latest.csv`
- `backend/output/act_classificacao_latest.csv`
- `backend/output/memorando_normalizado_latest.csv`
- `backend/output/documento_administrativo_normalizado_latest.csv`
- `backend/output/ted_normalizado_latest.csv`
- `backend/output/divergence_matrix_latest.csv`
- `backend/output/normalization_review_queue_latest.csv`
- `backend/output/performance_analysis.json`
- `output/execution_log_latest.json`

Modelo canonico em memoria:

- `processo`
- `processo_normalizado`
- `chave_canonica`
- `situacao_carteira`
- `documento_principal_tipo`
- `documento_principal_numero`
- `documentos_relacionados`
- `parceiro`
- `objeto_resumo`
- `objeto_completo`
- `vigencia_inicio`
- `vigencia_fim`
- `dias_restantes`
- `indicador_vigencia`
- campos de PT, TED, memorando, qualidade, origem e conflitos

## Dashboard

O dashboard:

1. le os arquivos `latest` de forma segura;
2. obtem a data real da ultima coleta pelo log, com fallback para `captured_at` e mtime;
3. monta modelos independentes para `Parcerias Vigentes`, `Termo de Execucao Descentralizada` e `Parcerias Descontinuadas / Nao Realizadas`;
4. calcula vigencia com a regra centralizada apenas onde ha acompanhamento operacional de prazo;
5. renderiza exatamente as tres categorias principais, sem total geral ou metricas misturadas.

Se uma fonte nao existir ou estiver vazia, a dashboard mostra estado vazio e cobertura, sem dados de exemplo.

## Separacao arquitetural atual

A camada gerencial fica no pacote `dashboard/` e nao altera o pipeline Selenium nem os arquivos bronze, silver e gold existentes. O entrypoint `dashboard_streamlit.py` apenas renderiza os modelos preparados por categoria. Se for necessario publicar um artefato canonico persistido no futuro, ele deve ser criado de forma aditiva a partir do mesmo contrato em memoria.
