# Orquestracao

## Estado atual

A orquestracao ainda e baseada em execucao manual de dois comandos separados:

1. `python backend/main.py`
2. `streamlit run dashboard_streamlit.py`

O backend ja executa um fluxo assistido completo dentro do SEI, mas nao publica automaticamente um dataset no contrato do dashboard.

## Fluxo operacional real

### Etapa 1. Backend

1. Operador executa `python backend/main.py`.
2. O backend carrega `.env`, configura logs e sobe o Chrome WebDriver.
3. O scraper limpa os artefatos anteriores em `backend/output/`.
4. O scraper abre o SEI e conclui login manual ou automatico.
5. O scraper navega para `Bloco > Interno`.
6. O scraper filtra os internos por `DESCRICOES_BUSCA`.
7. Para cada interno selecionado:
   - entra no interno;
   - gera a previa de `PARCERIAS VIGENTES`, quando aplicavel;
   - lista os processos;
   - abre cada processo;
   - abre `Pesquisar no Processo`;
   - busca `PLANO DE TRABALHO - PT`;
   - abre o resultado mais recente;
   - extrai o snapshot do documento;
   - salva JSON e CSVs auxiliares.
8. Ao final da rodada, o backend gera os CSVs normalizados de PT.
9. O backend fecha o navegador.

### Etapa 2. Dashboard

1. Operador executa `streamlit run dashboard_streamlit.py`.
2. O dashboard tenta ler `output/sei_dashboard.csv`.
3. Se nao encontrar esse arquivo, usa um dataset de exemplo.

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
- `pt_fields_raw.csv`
- `pt_status_execucao_latest.csv`
- `pt_sem_prazo_latest.csv`
- `pt_normalizado_latest.csv`
- `pt_normalizado_completo_latest.csv`

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

O gargalo de orquestracao nao esta na navegacao do SEI. Ele esta na ausencia de uma etapa que publique um dataset final no contrato do dashboard.

Hoje a sequencia correta e:

- coleta assistida e normalizacao no backend;
- analise manual dos artefatos em `backend/output/`;
- opcionalmente, transformacao externa para `output/sei_dashboard.csv`.

## Proxima etapa recomendada

Criar um passo de publicacao pos-coleta com as seguintes responsabilidades:

1. Ler `backend/output/pt_normalizado_latest.csv`.
2. Mapear para o schema canonico do dashboard.
3. Popular `documento`, `fonte`, `status` e `collected_at`.
4. Gerar `output/sei_dashboard.csv` na raiz.
5. Validar colunas e tipos antes de publicar.

## Validacoes operacionais recomendadas

- Falhar rapido se `SEI_URL` estiver ausente.
- Avisar explicitamente quando `DESCRICOES_BUSCA` estiver vazio ou nao selecionar nenhum interno.
- Logar o caminho final dos artefatos gerados.
- Logar quando o PT foi encontrado via busca e quando foi necessario fallback pela arvore.
- Distinguir no log o modo de extracao: `html_dom`, `pdf_native`, `pdf_ocr` ou equivalente.
