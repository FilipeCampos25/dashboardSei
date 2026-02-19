# Orquestracao

## Panorama
Atualmente a orquestracao e manual, com dois comandos separados:

1. Backend para autenticar no SEI.
2. Streamlit para visualizacao.

Nao existe scheduler ou pipeline automatizado no codigo atual.

## Fluxo operacional atual
1. Operador executa `python backend/main.py`.
2. Script inicializa driver, realiza login e finaliza.
3. Operador executa `streamlit run dashboard_streamlit.py`.
4. Dashboard carrega CSV local se existir; caso contrario usa mock interno.

## Fluxo alvo (ja suportado parcialmente pelos modulos)
1. Executar `SEIScraper.run()` para login, navegacao e coleta.
2. Persistir resultado com `ReportBuilder` em `output/sei_dashboard.csv`.
3. Iniciar Streamlit apontando para o arquivo atualizado.

## Contratos entre etapas
- Saida esperada da coleta: lista de dicionarios com timestamp e campos de negocio.
- Artefato de integracao: `output/sei_dashboard.csv`.
- Entrada do dashboard: CSV com colunas canonicas ou aliases reconhecidos.

## Dependencias de orquestracao
- `.env` valido.
- XPaths de login e navegacao atualizados.
- XPath `coleta.linhas_tabela` definido para captura real.
- Permissao de escrita em `output/`.

## Proposta minima de automacao (proximo passo)
- Criar um script unico (ex.: `run_pipeline.py`) que:
1. Carrega configuracao.
2. Executa `SEIScraper.run()`.
3. Salva CSV com `ReportBuilder`.
4. Opcionalmente inicia dashboard.

## Validacoes recomendadas no pipeline
- Falha rapida se `username/password` estiverem vazios.
- Falha clara se `coleta.linhas_tabela` nao estiver configurado.
- Logar quantidade de registros coletados e caminho final do arquivo.
