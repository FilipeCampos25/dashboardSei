# Orquestracao

## Estado atual

A orquestracao continua baseada em execucao manual de dois comandos separados:

1. `python backend/main.py`
2. `streamlit run dashboard_streamlit.py`

O backend executa uma rodada multi-documento no SEI e gera artefatos em camadas `bronze/silver/gold` dentro de `backend/output/`. A dashboard consome esses artefatos diretamente, junto com `output/execution_log_latest.json`.

## Fluxo operacional real

### Etapa 1. Backend

1. Operador executa `python backend/main.py`.
2. O backend carrega `.env`, configura logs e sobe o Chrome WebDriver.
3. O scraper limpa os artefatos `latest` anteriores em `backend/output/`.
4. O scraper abre o SEI e conclui login manual ou automatico.
5. O scraper navega para `Bloco > Interno`.
6. O scraper filtra os internos por `DESCRICOES_BUSCA`.
7. Para cada interno selecionado:
   - entra no interno;
   - gera a previa de `PARCERIAS VIGENTES`, quando aplicavel;
   - lista os processos;
   - abre cada processo;
   - abre `Pesquisar no Processo`;
   - tenta localizar, nesta ordem, os tipos documentais configurados;
   - itera candidatos do filtro por alias e, se necessario, faz fallback para a arvore;
   - extrai snapshot do documento;
   - classifica semanticamente o snapshot;
   - salva JSON bruto e registra o resultado em silver ou gold.
8. Ao final da rodada, o backend consolida os CSVs por familia documental.
9. O backend fecha o navegador.

### Etapa 2. Dashboard

1. Operador executa `streamlit run dashboard_streamlit.py`.
2. O dashboard le os arquivos `backend/output/*_latest` e o log da ultima rodada.
3. O dashboard monta a carteira canonica, separa ativos, historico e inconsistencias.
4. Se alguma fonte estiver ausente ou vazia, mostra estado vazio e cobertura, sem usar dados artificiais.

## Familias documentais processadas hoje

O backend procura e classifica:

- `pt`
- `act`
- `memorando`
- `ted`

Cada familia tem:

- busca por filtro do SEI com `filter_type_aliases`;
- fallback pela arvore do processo;
- classificacao semantica do snapshot;
- persistencia de artefatos em silver;
- publicacao em gold apenas para candidatos canonicamente validados.

## Modelo operacional de camadas

### Bronze

Mantem os JSONs brutos dos snapshots efetivamente capturados:

- `plano_trabalho_<processo>.json`
- `acordo_cooperacao_tecnica_<processo>.json`
- `memorando_<processo>.json`
- outros snapshots da familia de cooperacao, conforme o tipo resolvido

Nada e descartado nessa camada.

### Silver

Mantem rastreabilidade da rodada, inclusive:

- candidatos rejeitados semanticamente;
- minutas;
- documentos relacionados, mas nao canonicos;
- falhas de extracao;
- `not_found`;
- problemas de prazo em PT.

Arquivos principais:

- `pt_auditoria_latest.csv`
- `act_status_execucao_latest.csv`
- `memorando_status_execucao_latest.csv`
- `ted_status_execucao_latest.csv`

### Gold

Publica apenas registros canonicamente validados para a familia pedida.

Arquivos principais:

- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`
- `act_normalizado_latest.csv`
- `memorando_normalizado_latest.csv`

Hoje nao ha publicacao gold para TED porque a rodada mais recente nao localizou candidatos canonicos.

Observacao: quando `ted_normalizado_latest.csv` existir, a dashboard usa seus campos financeiros e de vigencia na aba `Termo de Execucao Descentralizada`.

## Comportamento de depuracao

O backend possui um desvio util para investigacao manual:

- `--no-stop-at-filter`

Com essa flag, depois de abrir o filtro do processo e localizar o contexto de pesquisa, o scraper mantem a aba aberta e interrompe o loop, em vez de fechar e seguir para o proximo processo.

## Contratos entre etapas

### Contrato efetivamente produzido hoje

Diretorio:

- `backend/output/`

Arquivos principais:

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
- `ted_normalizado_latest.csv`
- `dashboard_ready_latest.csv`
- `divergence_matrix_latest.csv`
- `normalization_review_queue_latest.csv`
- `parcerias_descontinuadas_normalizado_latest.csv`

### Contrato consumido pelo dashboard

Arquivos:

- `backend/output/dashboard_ready_latest.csv`
- `backend/output/*_normalizado_latest.csv`
- `backend/output/*_status_execucao_latest.csv`
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
- campos especializados de PT, TED, memorando, qualidade, origem e conflitos

## Separacao atual

O pipeline de coleta permanece separado da visao gerencial. A consolidacao da carteira acontece na camada de leitura da dashboard e nao reescreve os arquivos gold.

Hoje a sequencia correta e:

- coleta assistida e classificacao no backend;
- analise dos artefatos em `backend/output/`;
- execucao da dashboard gerencial sobre a ultima rodada.

## Proxima etapa recomendada

Se for necessario persistir a carteira canonica no futuro, criar um passo aditivo de publicacao pos-coleta com as seguintes responsabilidades:

1. Ler as fontes `latest` em `backend/output/`.
2. Reusar as regras de `dashboard_portfolio.py` e `dashboard_metrics.py`.
3. Preservar a separacao entre ativos, historico e inconsistencias.
4. Popular origem, qualidade, conflitos e `data_ultima_coleta`.
5. Gerar um artefato novo sem substituir os outputs atuais.
6. Validar colunas, tipos e contagens antes de publicar.

## Validacoes operacionais recomendadas

- Falhar rapido se `SEI_URL` estiver ausente.
- Avisar explicitamente quando `DESCRICOES_BUSCA` estiver vazio ou nao selecionar nenhum interno.
- Logar o caminho final dos artefatos gerados.
- Logar quando um candidato foi aceito em gold ou retido apenas na silver.
- Distinguir no log o modo de extracao: `html_dom`, `pdf_native`, `pdf_ocr`, `zip_docx` ou equivalente.
- Distinguir no log se o documento veio de filtro, alias de filtro ou fallback pela arvore.
