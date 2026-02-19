# Filosofia de Design

## Objetivo do projeto
O projeto prioriza entrega incremental: primeiro garantir automacao de acesso ao SEI, depois evoluir para coleta estruturada e visualizacao analitica.

## Principios adotados
- Simplicidade operacional: entrada por `.env`, dependencias diretas e comandos curtos.
- Separacao de responsabilidades: configuracao, driver, scraping, relatorio e dashboard em modulos distintos.
- Tolerancia a dados incompletos: dashboard aceita aliases de colunas e preenche faltantes.
- Evolucao guiada por contrato de dados: schema canonico no dashboard orienta coleta futura.
- Observabilidade basica: logs centralizados com nivel configuravel.

## Decisoes praticas atuais
- Backend usa Selenium com ChromeDriver gerenciado automaticamente (`webdriver-manager`).
- Dashboard nao depende do backend em tempo real; le CSV local (`output/sei_dashboard.csv`).
- Quando nao existe CSV, o dashboard sobe com dados de exemplo para manter demonstracao funcional.
- Fluxo de login foi isolado em `run_login_only()` para validar acesso antes de ampliar coleta.

## Trade-offs assumidos
- XPaths absolutos aceleram o inicio, mas sao mais sensiveis a mudancas na UI do SEI.
- Ausencia de orquestracao automatica reduz complexidade agora, mas exige execucao manual.
- `ReportBuilder` ainda sem integracao no fluxo principal evita acoplamento prematuro, mas deixa persistencia incompleta.

## Diretrizes para evolucao
- Conectar `SEIScraper.run()` ao entrypoint com persistencia CSV/XLSX.
- Definir e manter `coleta.linhas_tabela` no JSON de seletores.
- Migrar de extracao textual (`linha`) para campos estruturados nativos do scraper.
- Adicionar validacoes de schema antes da escrita de arquivos.
- Cobrir fluxo critico com testes minimos de regressao (configuracao e normalizacao de dados).
