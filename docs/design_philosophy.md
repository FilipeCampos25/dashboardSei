# Filosofia de Design

## Objetivo

Automatizar a coleta assistida de dados no SEI com foco em documentos de parceria e Plano de Trabalho, preservando rastreabilidade suficiente para depuracao e evolucao incremental do parser.

## Principios aplicados no codigo atual

### 1. Navegacao resiliente antes de analise sofisticada

O projeto prioriza chegar ao documento correto de forma repetivel:

- login manual como padrao;
- centralizacao de seletores em JSON;
- tolerancia a `iframes`, pop-ups e mudancas de contexto;
- fallbacks para busca do PT pela arvore do processo.

### 2. Persistencia em camadas

A coleta nao depende de um unico formato final. O sistema salva:

- previa operacional de `PARCERIAS VIGENTES`;
- snapshots JSON do PT;
- CSV raw para investigacao;
- relatorios de status da rodada;
- CSV normalizado para consumo tecnico posterior.

Isso reduz perda de informacao quando a normalizacao ainda nao captura todo o documento.

### 3. Extracao progressiva

A extracao segue uma escada de custo crescente:

1. DOM HTML do `iframe` de visualizacao
2. espera por renderizacao final
3. download do anexo
4. leitura nativa de PDF
5. OCR

O desenho privilegia a melhor fonte disponivel sem bloquear a rodada inteira por um unico formato de documento.

### 4. Normalizacao heuristica com rastreio

O parser de PT e necessariamente heuristico. Em vez de esconder essa limitacao, o sistema:

- preserva texto bruto;
- preserva tabelas;
- guarda caminhos dos JSONs;
- classifica o nivel de completude do registro normalizado.

### 5. Separacao entre coleta e visualizacao

O dashboard nao depende do backend em tempo real. Isso simplifica a operacao e evita acoplamento prematuro, mas cria uma lacuna de publicacao que ainda precisa ser resolvida.

## Trade-offs assumidos

- `xpath_selector.json` acelera manutencao, mas continua sensivel a mudancas de UI do SEI.
- Login manual reduz problemas de autenticacao complexa, mas exige operador.
- Limpeza do diretório de saida no inicio da rodada simplifica leitura dos `latest`, mas remove historico local.
- Normalizacao baseada em texto e tabelas aumenta cobertura, mas pode gerar classificacao parcial quando o documento nao segue padrao.
- O dashboard consumir um contrato diferente do backend protege a interface analitica, mas introduz um passo de integracao ainda ausente.

## Consequencias praticas

O valor do sistema hoje esta em:

- localizar o PT mais recente por processo;
- capturar evidencia reutilizavel do documento;
- gerar uma base normalizada inicial para evolucao do parser.

O valor ainda nao entregue de forma automatica e:

- publicar diretamente um `output/sei_dashboard.csv` pronto para o Streamlit.

## Direcao de evolucao

- Consolidar um publisher do backend para o contrato do dashboard.
- Adicionar testes unitarios para parsing de periodo, parceiro e objeto.
- Reduzir duplicidade de heuristicas de datas entre extrator e normalizador.
- Padronizar melhor campos com texto mojibake quando o SEI retornar conteudo com encoding inconsistente.
