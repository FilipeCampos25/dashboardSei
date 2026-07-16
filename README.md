# dashboard_sei

Automacao Selenium para coleta assistida no SEI e dashboard Streamlit para analise da ultima rodada de coleta.

O projeto executa uma rodada controlada no SEI, captura documentos e previas de processos, normaliza os dados em CSV/JSON dentro de `backend/output/` e exibe os resultados em uma dashboard local.

## Requisitos

- Python 3.11+
- Google Chrome instalado
- Acesso ao SEI usado pelo projeto
- Credenciais do SEI, quando for usar login automatico
- Dependencias Python listadas em `requirements.txt`
- Opcional para fallback de anexos/PDF:
  - Tesseract OCR
  - Poppler

Em Windows, configure `TESSERACT_CMD` e `POPPLER_PATH` no `.env` caso OCR ou conversao de PDF nao estejam no `PATH`.

## Instalacao

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

Depois edite o `.env` com a URL do SEI, credenciais e filtros da rodada.

Variaveis principais:

- `SEI_URL`: URL inicial do SEI.
- `SEI_USERNAME` e `SEI_PASSWORD`: credenciais usadas no login automatico.
- `MANUAL_LOGIN`: quando `true`, aguarda o operador concluir login/autenticacao.
- `MANUAL_LOGIN_WAIT_SECONDS`: tempo maximo de espera do login manual.
- `HEADLESS`: executa o Chrome sem interface quando `true`.
- `TIMEOUT_SECONDS`: timeout base das esperas Selenium.
- `LOG_LEVEL` e `DEBUG`: controle de logs.
- `OUTPUT_DIR`: diretorio de saida relativo a `backend/`; por padrao gera em `backend/output/`.
- `DESCRICOES_BUSCA`: descricoes dos internos que entram na coleta, separadas por `|`.
- `DESCRICOES_MATCH_MODE`: `contains` ou `equals`.
- `DOCUMENT_TYPES`: tipos documentais habilitados, separados por virgula. Valores atuais: `pt`, `act`, `memorando`, `ted`.
- `EXPORT_RAW_FIELDS_CSV`: quando ligado, gera `pt_fields_raw.csv`.
- `TESSERACT_CMD` e `POPPLER_PATH`: caminhos opcionais para OCR/PDF.

Exemplo:

```env
SEI_URL=https://...
MANUAL_LOGIN=true
HEADLESS=false
DESCRICOES_BUSCA=PARCERIAS VIGENTES|PARCERIAS DESCONTINUADAS / NAO REALIZADAS|TERMO DE EXECUCAO DESCENTRALIZADA
DESCRICOES_MATCH_MODE=contains
DOCUMENT_TYPES=pt,act,memorando,ted
```

## Como rodar

### Coleta pelo backend

Execucao padrao:

```powershell
python backend/main.py
```

Forcando login manual:

```powershell
python backend/main.py --manual-login
```

Tentando login automatico:

```powershell
python backend/main.py --auto-login
```

Rodada limitada para teste:

```powershell
python backend/main.py --manual-login --max-internos 1 --max-processos 2
```

Depuracao mantendo a aba do processo aberta:

```powershell
python backend/main.py --manual-login --no-stop-at-filter
```

Flags disponiveis:

- `--debug`: forca logs em DEBUG.
- `--manual-login`: aguarda login manual.
- `--auto-login`: tenta preencher login automaticamente.
- `--max-internos N`: limita a quantidade de internos processados.
- `--max-processos N`: limita a quantidade de processos por interno.
- `--no-stop-at-filter`: mantem a aba aberta para depuracao manual e interrompe o loop.

### Dashboard

```powershell
streamlit run dashboard_streamlit.py
```

Por padrao, o Streamlit sobe em `http://localhost:8501`.

A dashboard e uma interface de leitura da ultima rodada. Para executar nova coleta, rode o backend pela CLI e depois recarregue a pagina do Streamlit.

### Testes

```powershell
python -m unittest discover -s tests
```

## Interfaces e endpoints

O projeto nao expoe API HTTP propria, FastAPI ou endpoints REST locais.

Interfaces locais disponiveis:

- CLI de coleta: `python backend/main.py`
- UI local: `streamlit run dashboard_streamlit.py`
- Arquivos de dados: CSV/JSON em `backend/output/` e logs em `output/`

Integracao externa consumida:

- `GET https://val-siconv.np.estaleiro.serpro.gov.br/maisbrasil-api/v1/services/public/processo-compra/consultar`
- Uso atual: consulta de TED no Transferegov.
- Parametros enviados: `numeroProcesso`, `numeroInstrumento`, `anoInstrumento`, `anoProcesso`.

### Postman

Para testar no Postman, importe a collection:

- `docs/dashboard_sei.postman_collection.json`

Request disponivel hoje:

- `Transferegov - Consultar TED`

Configure as variaveis da collection antes de executar:

- `transferegov_base_url`: `https://val-siconv.np.estaleiro.serpro.gov.br/maisbrasil-api/v1`
- `numeroProcesso`: numero do processo SEI somente com digitos.
- `numeroInstrumento`: numero do instrumento TED.
- `anoInstrumento`: ano do instrumento TED.
- `anoProcesso`: ano extraido do processo SEI.

Exemplo de chamada montada:

```http
GET {{transferegov_base_url}}/services/public/processo-compra/consultar?numeroProcesso={{numeroProcesso}}&numeroInstrumento={{numeroInstrumento}}&anoInstrumento={{anoInstrumento}}&anoProcesso={{anoProcesso}}
```

Observacao: `backend/main.py` e `dashboard_streamlit.py` nao entram no Postman porque nao sao endpoints HTTP; eles sao executaveis locais via CLI/Streamlit.

## Fluxo do sistema

1. O backend carrega `.env`, configura logs e inicializa o Chrome WebDriver.
2. A rodada limpa artefatos `latest` anteriores em `backend/output/`.
3. O scraper abre o SEI e conclui login manual ou automatico.
4. Navega para `Bloco > Interno`.
5. Filtra internos por `DESCRICOES_BUSCA`.
6. Resolve o perfil do interno:
   - `parcerias_vigentes`
   - `parcerias_descontinuadas`
   - `ted`
7. Para `PARCERIAS VIGENTES`, coleta a previa da grade e depois abre cada processo.
8. Para cada processo, abre todas as pastas, usa `Pesquisar no Processo`, tenta aliases por tipo documental e faz fallback pela arvore quando necessario.
9. Extrai snapshot do documento: texto, tabelas, URL, titulo e modo de extracao.
10. Classifica o snapshot e separa registros em camadas bronze/silver/gold.
11. Normaliza PT, ACT, Memorando, TED e parcerias descontinuadas quando aplicavel.
12. Gera `dashboard_ready_latest.csv`, matriz de divergencia, fila de revisao e analise de performance.
13. A dashboard carrega somente os arquivos `*_latest` da ultima rodada.

## Dados que o sistema foca em coletar

### Parcerias vigentes

Coleta a previa do interno `PARCERIAS VIGENTES` e usa essa base para orientar a busca documental.

Campos principais:

- processo
- parceiro
- vigencia
- numero do ACT/instrumento quando aparece na previa
- objeto
- descricao e sequencia do interno

Saida principal:

- `backend/output/parcerias_vigentes_latest.csv`

### Plano de Trabalho (PT)

Busca documentos de Plano de Trabalho e normaliza dados operacionais do plano.

Campos principais:

- processo
- documento
- parceiro
- objeto
- data de assinatura
- datas de assinatura
- vigencia
- prazo de inicio e fim
- atribuicoes
- metas
- acoes
- status de validacao/publicacao
- caminho do JSON capturado

Saidas principais:

- `plano_trabalho_<processo>.json`
- `pt_status_execucao_latest.csv`
- `pt_auditoria_latest.csv`
- `pt_fields_raw.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`
- `pt_sem_prazo_latest.csv`

### Acordo de Cooperacao Tecnica (ACT)

Busca candidatos de ACT, classifica semanticamente e publica apenas o ACT canonico quando validado.

Campos principais:

- processo
- numero do acordo
- orgao/convenente
- objeto
- data de inicio e fim de vigencia
- status de classificacao
- motivo de rejeicao de candidatos nao canonicos
- divergencia entre processo pesquisado e documento escolhido

Saidas principais:

- `acordo_cooperacao_tecnica_<processo>.json`
- `act_status_execucao_latest.csv`
- `act_classificacao_latest.csv`
- `act_field_diagnostics_latest.csv`
- `act_normalizado_latest.csv`
- `act_candidate_discoveries_latest.csv`: inventario dos candidatos vistos antes da abertura
- `act_candidate_inventory_latest.csv`: candidatos descobertos e extraidos, com scores e gates
- `act_shadow_comparison_latest.csv`: comparacao resumida entre o vencedor atual e o proposto
- `act_shadow_comparison_latest.json`: decomposicao completa, metricas e hashes de imutabilidade

O scoring sombra e somente diagnostico: ele nao altera o alias
`acordo_cooperacao_tecnica_<processo>.json` nem `act_normalizado_latest.csv`.

### Memorando / documento administrativo

Busca documentos administrativos relacionados a cooperacao, como memorando, oficio, despacho, informacao tecnica, nota tecnica e solicitacao.

Campos principais:

- processo
- documento
- modo de extracao
- texto/snapshot
- data de assinatura
- datas de assinatura
- status de validacao/publicacao

Saidas principais:

- `memorando_entendimentos_<processo>.json`
- `documento_administrativo_status_execucao_latest.csv`
- `documento_administrativo_normalizado_latest.csv`
- `memorando_status_execucao_latest.csv`
- `memorando_normalizado_latest.csv`

### Termo de Execucao Descentralizada (TED)

Busca TED no SEI e tambem consulta a API publica do Transferegov quando ha numero de instrumento.

Campos principais:

- processo
- numero do instrumento
- objeto
- valor global
- situacao
- UF
- motivo de ausencia quando nao ha numero de instrumento, ACT previo ou resultado de API

Saidas principais:

- `termo_execucao_descentralizada_<processo>.json`
- `ted_status_execucao_latest.csv`
- `ted_normalizado_latest.csv`
- `ted_field_diagnostics_latest.csv`

### Parcerias descontinuadas / nao realizadas

O perfil `parcerias_descontinuadas` e `preview-only`: ele acessa o interno e captura a listagem, mas nao abre cada processo para buscar PT/ACT/Memorando/TED dentro desses registros.

Campos normalizados principais:

- processo
- tipo
- parceiro
- vigencia
- objeto
- data de assinatura
- data de vencimento
- status normalizado
- categoria de status
- status de normalizacao

Saidas principais:

- `parcerias_descontinuadas_latest.csv`
- `parcerias_descontinuadas_normalizado_latest.csv`

Observacao: esses registros entram somente na aba `Parcerias Descontinuadas / Nao Realizadas`. Eles nao alimentam alertas de prazo da carteira ativa.

## Dashboard

A dashboard le a ultima rodada a partir dos arquivos `latest`. Nao ha historico entre execucoes e a data exibida no cabecalho vem de `output/execution_log_latest.json`, com fallback para metadados reais dos arquivos.

Fontes consumidas pela nova camada gerencial:

- `backend/output/parcerias_vigentes_latest.csv`
- `backend/output/dashboard_ready_latest.csv` apenas para enriquecimento de parcerias vigentes
- `backend/output/ted_normalizado_latest.csv`
- `backend/output/parcerias_descontinuadas_normalizado_latest.csv`
- `backend/output/parcerias_descontinuadas_latest.csv` apenas no detalhe historico
- `backend/output/pt_normalizado_latest.csv` e `backend/output/pt_auditoria_latest.csv`
- `backend/output/act_normalizado_latest.csv`
- `backend/output/memorando_normalizado_latest.csv`
- `backend/output/documento_administrativo_normalizado_latest.csv`
- `output/execution_log_latest.json`

Abas principais:

- `Parcerias Vigentes`: consulta de processo, instrumento, parceiro, objeto e vigencia, com regra centralizada de situacao por prazo.
- `Termo de Execucao Descentralizada`: consulta independente de TEDs, valor global, vigencia, objeto e unidades quando houver cobertura.
- `Parcerias Descontinuadas / Nao Realizadas`: consulta histórica pelo status operacional calculado, sem alertas de vigência. O status coletado permanece em `status_raw`; evidências e data de referência do cálculo ficam disponíveis para auditoria.

Filtros por aba:

- processo
- parceiro
- documento
- situacao de vigencia apenas em parcerias vigentes e TEDs
- presenca de PT/TED apenas em parcerias vigentes
- faixa de valor apenas em TEDs
- status/categoria e intervalo de datas apenas no historico
- busca textual

A implementacao da interface fica em `dashboard_streamlit.py`; leitura, limpeza, regras de negocio e modelos por categoria ficam no pacote `dashboard/`. A regra verde/amarelo/vermelho fica centralizada em `dashboard/vigencia_rules.py`.

## Informacoes validas para levantar em uma analise

Para diagnosticar uma rodada, os pontos mais uteis sao:

- tempo total e tempo medio por processo em `performance_analysis.json`;
- total de linhas, warnings e errors em `output/execution_log_latest.json`;
- quantidade de processos ativos, historicos e inconsistentes na carteira canonica;
- quantidade de processos no `dashboard_ready_latest.csv`;
- cobertura por tipo documental: PT, ACT, Memorando e TED;
- quantidade de gold, silver, partial, not_found e extraction_failure por tipo;
- divergencias de processo em ACT;
- tentativas e rejeicoes de ACT em `act_rejection_summary`;
- PT com metas, acoes e prazo estruturado;
- TED sem numero de instrumento, sem ACT previo ou sem resultado de API;
- campos ausentes nos normalizados e diagnostics;
- data real da ultima coleta registrada no log ou nos artefatos `*_latest`;
- se `parcerias_descontinuadas` foi coletado e quantos registros foram normalizados.

## Artefatos da rodada

Os arquivos `latest` sao sobrescritos/limpos no inicio de cada nova rodada.

Camadas:

- Bronze: JSON bruto por documento/processo.
- Silver: status operacional, auditoria, diagnosticos e candidatos rejeitados.
- Gold: CSVs normalizados usados pela dashboard e analises.

Arquivos consolidados importantes:

- `dashboard_ready_latest.csv`: base principal da dashboard por processo, incluindo melhores datas de assinatura e vigencia consolidadas.
- `divergence_matrix_latest.csv`: matriz de divergencias entre previa, PT, ACT e TED.
- `normalization_review_queue_latest.csv`: fila de pontos para revisao.
- `performance_analysis.json`: tempos, spans e eventos de performance.
- `output/execution_log_latest.json`: log estruturado da ultima execucao.

## Observacoes operacionais

- A dashboard trabalha apenas com a ultima rodada.
- Se um arquivo ainda nao existir, a dashboard mostra estado vazio em vez de dados de exemplo.
- `DOCUMENT_TYPES` controla os tipos documentais buscados, mas o perfil do interno tambem limita o que sera processado.
- Em `parcerias_vigentes`, o perfil busca `pt`, `act` e `memorando`.
- Em `parcerias_descontinuadas`, o perfil coleta somente a previa/listagem.
- Em `ted`, o perfil busca `ted` e pode consultar o Transferegov.
- Quando `DESCRICOES_BUSCA` esta vazio ou nao encontra internos, a coleta nao avanca para processos.
