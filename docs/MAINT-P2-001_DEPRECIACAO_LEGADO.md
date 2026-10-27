# MAINT-P2-001 — Inventário e depreciação controlada

## Resultado da análise de chamadas

A busca estática no código, testes e documentação encontrou três wrappers PT
somente nas próprias definições:

- `_find_plano_trabalho_link_in_tree`;
- `_abrir_plano_trabalho_pela_arvore`;
- `_buscar_e_abrir_plano_de_trabalho_mais_recente`.

Os equivalentes genéricos `_find_document_link_in_tree`,
`_open_document_via_tree` e `_buscar_e_abrir_documento_mais_recente` possuem
chamadas produtivas e cobertura offline. Os wrappers podem ainda ser acessados
indiretamente por consumidores externos, portanto não foram removidos.

## Ciclo de depreciação

Os três wrappers continuam delegando aos caminhos genéricos e agora emitem
`DeprecationWarning` com a substituição indicada. O teste focado comprova tanto
o aviso quanto a equivalência da delegação.

A remoção fica condicionada a um ciclo posterior que volte a pesquisar usos
diretos, reflexão (`getattr`) e consumidores externos conhecidos. Até lá, a
compatibilidade permanece preservada.

## Limites

Nenhum fluxo produtivo, contrato de saída, timeout, normalizador ou artefato de
baseline foi alterado. A análise e os testes foram exclusivamente offline.
