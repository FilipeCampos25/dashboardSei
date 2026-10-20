# Modo produtivo de busca documental

## Decisão vigente

O fluxo produtivo chamado por `SEIScraper._run_document_search_for_process` opera em
`tree_only` para todas as famílias documentais. Ele inventaria os links da árvore do
processo, aplica ranking e abre seletivamente os candidatos elegíveis. Esta decisão
descreve o comportamento atual; não autoriza coleta ou mudança de configuração em
produção.

## Destino dos caminhos alternativos

- **Filtro e aliases:** permanecem implementados como caminho legado/experimental e
  apoio de diagnóstico, mas não são chamados pelo loop produtivo. Não devem ser
  removidos antes do ciclo de depreciação e da verificação de chamadas indiretas de
  `MAINT-P2-001`.
- **TED via API Transferegov:** permanece uma integração dormente e explicitamente
  online. O loop produtivo não chama `_process_ted_via_api`; qualquer ativação futura
  exige decisão própria, autorização de rede e preservação da barreira
  `ensure_online_operation_allowed`.

## Evidência offline

Os testes de `tests/test_wait_optimizations.py` chamam o orquestrador com mocks para
ACT, PT, memorando e TED. Eles verificam que a árvore é usada, que filtro/aliases não
são acionados e que a TED API não é chamada, inclusive quando há ACT anterior. Assim,
o contrato produtivo é caracterizado sem Selenium real, SEI ou rede.

## Política de alteração

Um modo diferente, como `filter_then_tree` ou `api_then_tree`, não deve ser inferido
pela mera existência das rotinas. Sua ativação requer item específico, testes offline
do roteamento e autorização separada para qualquer validação online.
