# Filosofia de Design

## Objetivo

Automatizar a coleta assistida de dados no SEI com foco em documentos de parceria e Plano de Trabalho, preservando rastreabilidade suficiente para auditoria, depuracao e evolucao incremental do parser.

## Principios aplicados no codigo atual

### 1. Navegacao resiliente antes de analise sofisticada

O projeto prioriza chegar ao melhor candidato documental de forma repetivel:

- login manual como padrao;
- centralizacao de seletores em JSON;
- tolerancia a `iframes`, pop-ups e mudancas de contexto;
- aliases de filtro para taxonomias inconsistentes do SEI;
- fallback pela arvore do processo quando o filtro nao consolida candidato canonico.

### 2. Persistencia em camadas

A coleta nao depende de um unico formato final. O sistema salva:

- previa operacional de `PARCERIAS VIGENTES`;
- snapshots JSON brutos;
- CSV raw para investigacao;
- silver com todos os candidatos processados e seus descartes;
- gold apenas com registros canonicamente publicados.

Isso reduz perda de informacao quando o SEI esta mal classificado ou quando a normalizacao ainda nao captura todo o documento.

### 3. Extracao progressiva

A extracao segue uma escada de custo crescente:

1. DOM HTML do `iframe` de visualizacao
2. espera por renderizacao final
3. download do anexo
4. leitura nativa de PDF
5. OCR
6. leitura de DOCX ou `zip_docx`

O desenho privilegia a melhor fonte disponivel sem bloquear a rodada inteira por um unico formato de documento.

### 4. Classificacao semantica antes da publicacao

O tipo do SEI e tratado como pista, nao como verdade.

Por isso o sistema:

- busca candidatos por tipo e por alias;
- classifica semanticamente o conteudo capturado;
- distingue `valid_for_requested_type` de `related_but_not_canonical`;
- publica em gold apenas quando a semantica confirma o tipo pedido.

Isso evita promover minutas, extratos, documentacao ou documentos correlatos apenas porque o SEI os classificou de forma conveniente.

### 5. Normalizacao heuristica com rastreio

O parser de PT e necessariamente heuristico. Em vez de esconder essa limitacao, o sistema:

- preserva texto bruto;
- preserva tabelas;
- guarda caminhos dos JSONs;
- guarda `validation_status`, `publication_status` e `classification_reason`;
- registra `period_source` quando a vigencia vem de rotulo direto, de assinatura ou permanece inconclusiva.

### 6. Separacao entre coleta e visualizacao

O dashboard nao depende do backend em tempo real. Isso simplifica a operacao e evita acoplamento prematuro, mas cria uma lacuna de publicacao que ainda precisa ser resolvida.

## Trade-offs assumidos

- `xpath_selector.json` acelera manutencao, mas continua sensivel a mudancas de UI do SEI.
- Login manual reduz problemas de autenticacao complexa, mas exige operador.
- Limpeza do diretorio de saida no inicio da rodada simplifica leitura dos `latest`, mas remove historico local.
- Normalizacao baseada em texto, tabelas e assinaturas aumenta cobertura, mas continua sujeita a OCR ruim e documentos fora do padrao.
- O dashboard consumir um contrato diferente do backend protege a interface analitica, mas introduz um passo de integracao ainda ausente.

## Consequencias praticas

O valor do sistema hoje esta em:

- localizar candidatos documentais relevantes por processo;
- resistir a classificacao inconsistente do SEI;
- capturar evidencia reutilizavel do documento;
- separar claramente o que vai para silver do que pode subir para gold;
- gerar uma base normalizada inicial para evolucao do parser.

O valor ainda nao entregue de forma automatica e:

- publicar diretamente um `output/sei_dashboard.csv` pronto para o Streamlit.

## Direcao de evolucao

- Consolidar um publisher do backend para o contrato do dashboard.
- Adicionar cobertura de testes para casos reais de ambiguidade documental.
- Evoluir a familia de cooperacao quando houver necessidade de suportar novos instrumentos, como `Acordo de Parceria`.
- Reduzir duplicidade de heuristicas de datas entre extrator e normalizador.
- Padronizar melhor campos com texto mojibake quando o SEI retornar conteudo com encoding inconsistente.
