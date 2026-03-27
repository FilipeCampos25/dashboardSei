# Orquestracao

## Estado atual

A orquestracao continua baseada em execucao manual de dois comandos separados:

1. `python backend/main.py`
2. `streamlit run dashboard_streamlit.py`

O backend agora executa uma rodada multi-documento no SEI, gera artefatos em camadas `bronze/silver/gold` dentro de `backend/output/`, mas ainda nao publica automaticamente o contrato final consumido pelo dashboard em `output/sei_dashboard.csv`.

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
2. O dashboard tenta ler `output/sei_dashboard.csv`.
3. Se nao encontrar esse arquivo, usa um dataset de exemplo.

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

### Contrato esperado pelo dashboard

Arquivo:

- `output/sei_dashboard.csv`

Schema canonico:

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

## Gargalo atual

O gargalo de orquestracao nao esta na navegacao do SEI. Ele esta na ausencia de uma etapa de publicacao que consolide a gold do backend no contrato do dashboard.

Hoje a sequencia correta e:

- coleta assistida e classificacao no backend;
- analise dos artefatos em `backend/output/`;
- opcionalmente, transformacao externa para `output/sei_dashboard.csv`.

## Proxima etapa recomendada

Criar um passo de publicacao pos-coleta com as seguintes responsabilidades:

1. Ler a gold relevante em `backend/output/`.
2. Mapear os registros para o schema canonico do dashboard.
3. Definir a precedencia entre PT e familias de cooperacao.
4. Popular `documento`, `fonte`, `status` e `collected_at`.
5. Gerar `output/sei_dashboard.csv` na raiz.
6. Validar colunas e tipos antes de publicar.

## Validacoes operacionais recomendadas

- Falhar rapido se `SEI_URL` estiver ausente.
- Avisar explicitamente quando `DESCRICOES_BUSCA` estiver vazio ou nao selecionar nenhum interno.
- Logar o caminho final dos artefatos gerados.
- Logar quando um candidato foi aceito em gold ou retido apenas na silver.
- Distinguir no log o modo de extracao: `html_dom`, `pdf_native`, `pdf_ocr`, `zip_docx` ou equivalente.
- Distinguir no log se o documento veio de filtro, alias de filtro ou fallback pela arvore.
