# COL-P2-002 — Benchmark offline de waits

## Escopo

O benchmark usa somente drivers, relógios e sinais sintéticos de
`tests/test_wait_optimizations.py`. Ele não abre navegador real, não acessa o
SEI e não reduz timeouts de segurança do fluxo produtivo.

## Comparação determinística

O cenário agrega dois waits que antes consumiriam integralmente um segundo
cada: liberação de overlay e abertura de nova janela. Os mocks disponibilizam
o sinal esperado após 0,2 segundo em cada etapa.

| Estratégia | Espera simulada | Resultado funcional |
|---|---:|---|
| espera fixa até o limite | 2,0 s | overlay liberado e nova janela selecionada |
| espera condicional atual | 0,4 s | overlay liberado e nova janela selecionada |

A redução simulada é de 80%, com os mesmos resultados funcionais. O teste
`test_conditional_wait_benchmark_preserves_results_and_reduces_simulated_wait`
mede esses valores com relógio falso e também comprova que os sleeps de
fallback não são usados quando o sinal aparece dentro do prazo.

## Limites da evidência

Esta métrica compara lógica de espera, não tempo de uma coleta real. Os
fallbacks curtos e os timeouts existentes permanecem ativos para falhas e
sinais que chegam no limite. Qualquer ajuste adicional em ambiente real exige
autorização separada e nova evidência.
