# ADM-P2-001 — Benchmark do resumo administrativo

## Diagnóstico

O legado segmentava o texto somente em pontuação seguida de espaço, aceitava
frases com ao menos 30 caracteres, concatenava as duas primeiras elegíveis,
normalizava espaços e truncava em 600 caracteres. Não havia filtro de cabeçalho
ou protocolo, prioridade para conclusão/dispositivo nem fallback além do
resultado vazio. Texto vazio retornava vazio; texto curto só era mantido quando
possuía ao menos 30 caracteres.

Consumidores confirmados: CSV administrativo e alias legado de memorando,
sidecar V2/proveniência, reprocessador offline, carregadores do dashboard,
portfolio (fallback de pendências), e revisão de normalização. `resumo` é campo
opcional nos perfis administrativos. Classificação, decisão de document gold e
publication não usam seu conteúdo. Nenhum consumidor downstream foi alterado.

## Dataset e avaliação

O dataset reproduzível está em
`tests/benchmarks/administrativo_resumo_v1.json`: 12 exemplos sintéticos e
sanitizados (4 notas técnicas, 3 memorandos, 2 despachos e 3 ofícios), cobrindo
cabeçalho longo, protocolo inicial, histórico extenso, conclusão útil,
documento curto, vazio e texto sem pontuação.

As dimensões humanas, fixadas antes da decisão, são relevância, ausência de
contaminação, contexto, fidelidade e concisão, em escala 0–2. Regra de ativação:
ganho médio mínimo de 1 ponto, ao menos 4 melhorias, no máximo 1 regressão,
delta de fidelidade não negativo, nenhuma regressão grave e regras gerais
simples.

## Baseline legado

- Média: 7,17/10; mediana: 6,5/10.
- Casos bons (8–10): 6; aceitáveis (5–7): 6; ruins (<5): 0.
- Contaminação dominante: 3; incidental: 2.
- Modos de erro: `HEADER_FIRST` 3, `PROTOCOL_FIRST` 2,
  `HISTORY_FIRST` 3, `ACCEPTABLE` 3 e `EMPTY` 1.

## Candidato e comparação

O candidato preserva sentenças literais, remove somente prefixo estrutural
explícito antes de travessão, penaliza frases iniciadas por sinais gerais de
cabeçalho/protocolo e prioriza sinais de conclusão, recomendação, decisão,
solicitação e encaminhamento. Sem sinal positivo, usa até duas primeiras frases
não estruturais; em último caso preserva a primeira frase. Mantém o limite de
600 caracteres, é determinístico, offline e não adiciona dependências.

- Candidato: média 9,75/10; mediana 10/10.
- Ganho médio: +2,58 pontos.
- Comparação: 8 melhorias, 3 empates e 1 regressão leve; nenhuma grave.
- Fidelidade: 2,0 no legado e 2,0 no candidato.
- Decisão: **ATIVAR**.

A regressão leve ocorreu em memorando direto, no qual a frase principal foi
preservada e uma segunda frase explicativa deixou de ser incluída. O ganho
global, a remoção dos erros dominantes e a fidelidade estável superaram a regra
conservadora predefinida.

## Reprocessamento offline e compatibilidade

Os 22 snapshots administrativos históricos foram copiados e reprocessados em
diretório temporário, uma vez com legado e outra com candidato: 22/22
processados em cada execução, sem falha ou unresolved. Houve 12 resumos iguais
e 10 diferentes; o total caiu de 4.582 para 3.194 caracteres. Todos os demais
campos, inclusive classe, requested type, datas, assunto, ação, prazo,
referências, estados, proveniência e decisão de gold, ficaram idênticos em
22/22. Melhorias/empates/regressões qualitativas são as avaliações humanas do
benchmark sanitizado; não foram inferidas automaticamente dos históricos.

## Testes e integridade

- Benchmark focado: 6/6 aprovados após ativação.
- Administrativos focados: 49/49 aprovados antes da ativação final.
- Regressão proporcional: 128/128 aprovados fora do sandbox; a primeira
  execução no sandbox teve apenas 29 problemas de ACL em `TemporaryDirectory`.
- Suíte offline completa: 597 executados, 592 aprovados e 5 integrações online
  puladas, sem falhas ou erros.
- `compileall`, higiene de segredos e `git diff --check`: verificados no
  fechamento operacional do item.
- Inventário protegido inicial: 136 arquivos, 2.025.623 bytes, digest SHA-256
  consolidado `f06011648da442a504d440f6ec635dc46abe0922427e559bd902d95211818e00`.

## Limitações

O dataset é pequeno e intencionalmente auditável; a heurística não pretende
ser sumarização semântica genérica. Frases úteis sem qualquer sinal prioritário
continuam usando ordem documental. O diretório temporário criado para o
reprocessamento não integra o produto nem os artefatos protegidos.
