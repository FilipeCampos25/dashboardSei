# Desenvolvimento

## Pre-requisitos
- Python 3.11+
- Google Chrome
- Acesso ao SEI

## Instalacao
```bash
pip install -r requirements.txt
```

## Configuracao (`.env`)
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
- `DESCRICOES_BUSCA` (ex.: `PARCERIAS VIGENTES|TED|ACT`)
- `DESCRICOES_MATCH_MODE` (`contains` ou `equals`)

Observacoes:
- `DESCRICOES_BUSCA` e importante para a selecao guiada de internos. Se vazio, o fluxo nao seleciona internos.
- `OUTPUT_DIR` no backend e resolvido relativamente a pasta `backend/` (padrao gera arquivos em `backend/output/`).
- `CHROMEDRIVER_PATH` e opcional (fallback para ambientes onde Selenium Manager nao resolve o driver).

## Rodar backend (fluxo assistido)
Padrao (usa `MANUAL_LOGIN` do `.env`):
```bash
python backend/main.py
```

Forcando login manual:
```bash
python backend/main.py --debug --manual-login
```

Tentando login automatico:
```bash
python backend/main.py --debug --auto-login
```

Limitando escopo para testes:
```bash
python backend/main.py --manual-login --max-internos 1 --max-processos 2
```

## O que o backend faz hoje
- Abre o SEI e executa login (manual ou automatico).
- Navega em `Bloco > Interno`.
- Filtra internos pelas descricoes configuradas.
- Para internos `PARCERIAS VIGENTES`, gera preview CSV com `processo`, `parceiro`, `vigencia`, `objeto`, `numero_act`.
- Percorre processos e coleta nomes dos documentos.

## Destaque de coletagem (escopo funcional)
O fluxo suporta a coletagem voltada a:

- parcerias vigentes
- Memorando de Entendimento
- TED
- ACT
- Plano de Trabalho (metas, acoes e prazos no dataset analitico)

Importante:
- Hoje a coleta estruturada implementada diretamente no scraper esta focada em `PARCERIAS VIGENTES`.
- Memorando/TED/ACT/Plano de Trabalho aparecem hoje na varredura dos documentos dos processos; a estruturacao detalhada desses documentos ainda depende da etapa de integracao/pipeline.

## Saidas geradas
- Preview de parcerias vigentes: `backend/output/parcerias_vigentes_YYYYMMDD_HHMMSS.csv`
- Lista de documentos encontrados: mantida em memoria/log (retorno de `run_full_flow`)

## Rodar dashboard
```bash
streamlit run dashboard_streamlit.py
```

## Entrada esperada do dashboard
O dashboard procura `output/sei_dashboard.csv` (na raiz do repositorio).

Se o arquivo nao existir:
- exibe warning;
- sobe com dados de exemplo.

## Gap atual de integracao
O backend gera preview em `backend/output/`, enquanto o dashboard le `output/sei_dashboard.csv` na raiz. Para integrar ponta a ponta, e necessario criar uma etapa de transformacao/persistencia no formato canonico do dashboard.
