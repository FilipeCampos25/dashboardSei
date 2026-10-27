# MAINT-P2-002 — Aliases PT e gates de release

## Contrato dos aliases

`pt_normalizado_latest.csv` é o artefato PT canônico. O arquivo
`pt_normalizado_completo_latest.csv` não representa outro subconjunto: ele é um
alias legado mantido como cópia byte a byte para não quebrar consumidores
existentes.

O exporter grava o canônico uma única vez e deriva o alias por cópia. O teste
do normalizador compara os bytes dos dois arquivos.

## Gates de release

O empacotador de release falha fechado quando encontra nomes duplicados no ZIP
ou material de segredo de alta confiança. Os testes cobrem explicitamente os
dois casos. Outputs gerados, incluindo ambos os nomes PT, continuam fora da
allowlist do pacote.

## Compatibilidade

Nenhuma coluna, linha, ordenação ou caminho legado foi removido. Consumidores
novos devem usar o nome canônico; a retirada do alias exige ciclo de depreciação
separado.
