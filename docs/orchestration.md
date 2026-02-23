# Orquestracao

## Panorama atual
A orquestracao ainda e operacional/manual, mas o backend ja executa um fluxo assistido completo (nao apenas login).

Comandos separados:
1. Backend (`python backend/main.py`) para navegacao/coleta no SEI.
2. Dashboard (`streamlit run dashboard_streamlit.py`) para analise.

## Fluxo operacional atual (real)
1. Operador executa `python backend/main.py`.
2. Backend carrega `.env`, configura logs e sobe o Chrome WebDriver.
3. `SEIScraper.run_full_flow(...)`:
   - login manual/automatico;
   - fechamento de pop-up;
   - menu `Bloco > Interno`;
   - selecao guiada de internos por `DESCRICOES_BUSCA`;
   - coleta preview de `PARCERIAS VIGENTES` (quando aplicavel);
   - abertura de processos e coleta de documentos na arvore.
4. Backend encerra o driver.
5. Operador executa `streamlit run dashboard_streamlit.py`.
6. Dashboard tenta ler `output/sei_dashboard.csv`; se nao existir, usa dataset de exemplo.

## Destaque de coletagem no fluxo
O fluxo de orquestracao foi desenhado para suportar coletagem de:

- parcerias vigentes
- Memorando de Entendimento
- TED
- ACT
- Plano de Trabalho (metas, acoes e prazos)

Estado atual por etapa:
- Backend (preview estruturado): `PARCERIAS VIGENTES` com `processo`, `parceiro`, `vigencia`, `objeto`, `numero_act` (ACT).
- Backend (varredura de documentos): captura nomes dos documentos dos processos, incluindo documentos como Memorando/TED/ACT/Plano de Trabalho.
- Dashboard (consumo): schema pronto para armazenar `documento`, `atribuicao`, `meta`, `acao`, `prazo`, `status`, `fonte`.

## Contratos entre etapas (atual vs alvo)
### Contrato atual efetivamente produzido
- Arquivo preview: `backend/output/parcerias_vigentes_*.csv`
- Colunas: `interno_descricao`, `seq`, `processo`, `parceiro`, `vigencia`, `numero_act`, `objeto`

### Contrato alvo para o dashboard
- Arquivo: `output/sei_dashboard.csv` (raiz)
- Colunas canonicas:
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

## Dependencias operacionais
- `.env` valido (especialmente `SEI_URL`, login e `DESCRICOES_BUSCA`)
- `xpath_selector.json` atualizado conforme a UI do SEI
- Google Chrome instalado
- Permissao de escrita em `backend/output/` (preview) e/ou `output/` (pipeline dashboard)

## Proposta minima de orquestracao (proximo passo)
1. Criar etapa de transformacao do preview `parcerias_vigentes_*.csv` para o schema canonico do dashboard.
2. Persistir em `output/sei_dashboard.csv` na raiz.
3. Padronizar `documento`/`fonte` para distinguir Memorando de Entendimento, TED, ACT e Plano de Trabalho.
4. Extrair `vigencia_inicio` e `vigencia_fim` a partir de `vigencia`.
5. Evoluir parser dos documentos para preencher `atribuicao`, `meta`, `acao` e `prazo`.

## Validacoes recomendadas
- Falha rapida se `DESCRICOES_BUSCA` estiver vazio.
- Logar internos selecionados e quantidade de processos percorridos.
- Logar caminho do CSV preview gerado em `backend/output/`.
- Validar schema antes de gravar `output/sei_dashboard.csv`.
