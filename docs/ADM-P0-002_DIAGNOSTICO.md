# ADM-P0-002 — Data administrativa contextual

## Diagnóstico confirmado

`_extract_data` usava `re.search(DATE_PATTERN, text)` e retornava a primeira
expressão encontrada. O builder V2 promovia esse valor a `PRESENT`, com a regra
`administrativo.data.first_match`. Isso confirma a causa: posição textual era
suficiente, sem distinguir data documental, norma, documento citado ou assinatura.

`data_assinatura` e `datas_assinatura` usam o helper compartilhado
`_extract_signature_dates`, normalizam para ISO e preservam, respectivamente,
a maior data detectada e todas as datas. Não existe contrato declarando
equivalência com `data`; ambos foram preservados sem alteração.

Precondições: ADM-P0-001, ADM-P1-001 e TEST-P1-001 estão VALIDATED.
ADM-P1-002 tem divergência documental: seu campo Status diz IMPLEMENTED, mas sua
Evidência de validação registra autorização explícita e o controle de progresso
confirma VALIDATED/commit 283a013. Essa evidência confirma a precondição; nenhum
campo do item anterior foi corrigido nesta tarefa.

## Caracterização anterior e política

22 administrativos: 15 notas técnicas, 3 despachos, 2 memorandos e 2 ofícios.
10 tinham data principal; 12 estavam sem texto e sem data. Foram encontrados
datas normativas, documentos mencionados, assinatura eletrônica, linhas de
cidade/data no início ou bloco final e datas de impressão do documento.
Não há metadado HTML confiável de data documental no snapshot disponível.
Os dados não sustentam uma política distinta por classe: os memorandos e um
ofício estão vazios, e os sinais de cidade/data atravessam outras classes.

A correção é deliberadamente V2, seguindo o padrão aditivo da Fase 6:

- linhas completas de `Data:`, `Data do documento:` ou `Data de emissão:`;
- linhas completas de cidade/data para Brasília (também DF), Porto Velho e
  São José dos Campos, localidades observadas na baseline;
- validação de calendário e normalização ISO pelo helper já existente;
- uma única data válida nesses contextos: PRESENT com evidência real;
- várias datas distintas nesses contextos: CONFLICT sem vencedor, preservando
  as evidências; repetições da mesma data não constituem conflito;
- candidatos sem contexto suficiente: UNRESOLVED, com evidências;
- nenhum candidato: estado da política existente. Para data optional, continua
  NOT_EVALUATED; falhas técnicas permanecem INACCESSIBLE/EXTRACTION_FAILED.

Os dois sinais documentais têm a mesma força; não há desempate por posição.
Normas, assinatura, prazo/período, documento/processo mencionado e histórico são
contextualizados, mas não elegíveis por si. Texto livre, localidade desconhecida,
citação inline ou linha marcada como citação também não é suficiente.
Não há fallback para processo, captured_at, coleta, filesystem, arquivo ou título.

## Comparação sanitizada e reprocessamento

Casos anonimizados por ordem dos 22 snapshots. O CSV legado conserva os valores
da coluna abaixo; o FieldResult V2 contém a resolução indicada.

| Caso | Classe | Data legada | Assinatura preservada | Data V2 / estado | Motivo |
|---|---|---|---|---|---|
| ADM-01 | nota_tecnica | 24/03/2021 | 30/05/2022 | UNRESOLVED | Portaria citada; sem linha documental |
| ADM-02 | despacho | 31/05/1989 | 04/10/2021 | 2021-09-20 / PRESENT | Linha Brasília/data; lei histórica rejeitada |
| ADM-07 | nota_tecnica | 23/09/2021 | 29/09/2021 | 2021-09-23 / PRESENT | Linha Brasília/data; data mantida |
| ADM-08 | oficio | 10/11/2025 | 10/11/2025 | 2025-11-10 / PRESENT | Linha São José dos Campos/data; mantida |
| ADM-09 | nota_tecnica | 28/05/2021 | 08/07/2026 | UNRESOLVED | Portaria citada; sem linha documental |
| ADM-10 | despacho | 14/04/2023 | 15/12/2023 | UNRESOLVED | Documento mencionado; sem linha documental |
| ADM-12 | despacho | 06/08/2025 | 06/08/2025 | UNRESOLVED | Somente assinatura; não declarar equivalência |
| ADM-18 | nota_tecnica | 05/01/2011 | 09/03/2020 | 2020-03-04 / PRESENT | Linha Porto Velho/data; decreto histórico rejeitado |
| ADM-19 | nota_tecnica | 05/01/2011 | 09/06/2020 | 2020-06-08 / PRESENT | Linha Porto Velho/data; decreto histórico rejeitado |
| ADM-20 | nota_tecnica | 22/12/2022 | 23/12/2022 | UNRESOLVED | Somente assinatura; não declarar equivalência |

ADM-03 a 06, 11, 13 a 17, 21 e 22: data e assinatura ausentes antes e depois;
data V2 NOT_EVALUATED, sem evidência fabricada. Os 12 vazios continuam non-gold.

Métricas: 22 processados; 2 datas mantidas semanticamente (representação ISO),
3 corrigidas, 5 antes presentes agora UNRESOLVED, 12 sem data, 0 ABSENT,
0 conflitos na baseline. Cinco primeiras datas normativas não foram promovidas
(ADM-01, 02, 09, 18, 19), incluindo todos os casos conhecidos de 1989/2011.
Os cinco PRESENT V2 possuem evidência `city_date`.

Reprocessamento real com `reprocess_snapshot(..., family='administrative')` em
`_tmp_adm_p0_002/reprocessed`, duas execuções, 22/22 processados e hashes idênticos.
Os 264 FieldResult reprocessados passaram em validate_field_provenance.
Classe, assinatura, data legada e decisão gold permaneceram iguais na comparação.
Snapshots históricos continuam bloqueados no V2 por estados técnicos incompletos.
Os arquivos temporários não alteram snapshots nem outputs históricos.

Artefatos locais sanitizados: `_tmp_adm_p0_002/before.json`, `after.json` e
`comparison.json`. A tabela anterior inclui todos os candidatos, raw de data,
posição e categoria contextual, sem trechos institucionais ou identidades.
O script local `_tmp_adm_p0_002/audit.py` reproduz os inventários/caracterização.
Exemplos de motivos comparativos: CONTEXTUAL_DATE,
INSUFFICIENT_DOCUMENT_CONTEXT, NO_CANDIDATES_OPTIONAL.

## Proveniência e compatibilidade

Usa exclusivamente FieldResult, FieldState, FieldEvidence e EvidenceLocation
existentes. SourceKind DOCUMENT; raw preservado; rule_id contextual; position
zero-based no texto normalizado do snapshot; section identifica a categoria;
source_path e identidade documental quando conhecidos. Conflito/abstenção não
contêm value vencedor. Ausência não fabrica evidência. O contrato não possui
campos de confidence/warning, e nenhum foi acrescentado.

CSV e normalizado legado permanecem compatíveis, inclusive `_extract_data`.
A alteração deliberada ocorre somente em `fields[data]` V2; assinatura e demais
campos não mudam. Os perfis e required fields de ADM-P1-002 não foram redefinidos;
quality_status_v2 continua aplicando a política existente a UNRESOLVED/CONFLICT.

## Alterações e revisão

- `backend/app/services/documento_administrativo_normalizer.py`: helpers
  `_date_context` / `_resolve_document_date` e integração específica de data no
  builder V2. Reutilização do normalizador de calendário existente.
- `tests/test_administrativo_dates.py`: 12 testes, com os sete casos obrigatórios,
  conflito independente da ordem, calendário inválido, citações e repetição.
- `tests/test_administrativo_provenance.py`: atualiza a expectativa V2 da data
  histórica para abstenção; mantém expectativas legadas e demais campos.
- Este relatório e campos operacionais somente de ADM-P0-002.

Diff rastreado: 78 inserções e 2 remoções em 2 arquivos já rastreados; adicionalmente
o novo teste (110 linhas) e este relatório. `git diff --stat` não inclui novos
arquivos ainda não adicionados. Nenhum commit/staging/push foi realizado.
Revisão integral confirmou escopo administrativo/data, imports, tratamento de
calendário inválido, ausência de debug novo, nenhuma dependência ou alteração de
classe, gold gate, campos required ou famílias ACT/PT/TED/descontinuadas.
`git diff --check` passou; avisos de conversão LF/CRLF não são erros.

## Testes e comandos

Todos com OFFLINE_ONLY=true e DEBUG=false, usando `.venv/Scripts/python.exe`.
Logs completos em `_tmp_adm_p0_002/tests_*.txt`.

| Comando / grupo | Executados | Aprovados | Falhas | Erros | Pulados | Classificação |
|---|---:|---:|---:|---:|---:|---|
| `-m unittest tests.test_administrativo_dates -v`, antes | 12 | 2 | 17 asserções/subtests | 0 | 0 | Bug reproduzido |
| Datas, provenance, field_policy, taxonomy | 27 | 27 | 0 | 0 | 0 | Focados aprovados |
| Primeira regressão no sandbox | 124 | 112 | 4 subtests | 12 | 0 | ACL em temporários; 1 erro de nome de módulo no comando |
| Regressão corrigida fora do sandbox | 139 | 139 | 0 | 0 | 0 | Aprovada |
| `-m unittest discover -s tests -p 'test_*.py' -v`, fora do sandbox | 575 | 570 | 0 | 0 | 5 | Integrações online puladas |

Regressão corrigida: `-m unittest tests.test_administrativo_dates
tests.test_administrativo_provenance tests.test_administrativo_field_policy
tests.test_administrativo_taxonomy tests.test_field_states tests.test_field_evidence
tests.test_gold_contracts tests.test_normalization_contract tests.test_contract_adapters
tests.test_semantic_states tests.test_pipeline_states tests.test_normalization_review
tests.test_offline_reprocessor tests.test_publication_policy
tests.test_document_gold_invariants tests.test_provenance_validation -v`.

`-m compileall -q backend tests`: exit 0, avisos de ACL somente nos temporários
preexistentes. `scripts/check_secret_hygiene.py`: aprovado, exit 0.
`git diff --check`: aprovado. Nenhuma coleta, login, Selenium ou rede institucional
foi executada pelo trabalho deste item.
Os dois arquivos novos também passaram na função unsafe_secret_lines do checker;
o teste novo foi compilado diretamente. A leitura auxiliar dos JSONs foi repetida
com UTF-8 explícito após erro de encoding cp1252 no primeiro comando de auditoria.

## Integridade antes/depois

Inventário SHA-256 consolidado por raiz, com path relativo UTF-8 seguido do hash
binário do conteúdo, em ordem lexical. Os três inventários permaneceram iguais,
incluindo snapshots, fixtures protegidas e golden-master dentro dessas raízes.

| Raiz | Quantidade antes = depois | Bytes antes = depois | Digest antes = depois |
|---|---:|---:|---|
| backend/output | 121 | 2.010.390 | eb9fe58dbdd6ed392692767532ffb782ecd8d1fae94d9a6d53b9a40b496ca4eb |
| output | 1 | 223 | 3646db4dd0f710f5a975575e53d8d6fb11fe95d551672dfa049a60cc122d8f6b |
| tests/fixtures | 14 | 15.010 | 7b5302c2d545662b819d34162c445414779777f1884cefcb8e258dd2218ba4bd |

Total: 136 arquivos, 2.025.623 bytes. Baseline externa privada não fornecida não
foi acessada; o reprocessamento cobre os 22 administrativos locais disponíveis.

## Limitações e achados fora do escopo

O legado conserva a extração antiga, deliberadamente. Consumidores da correção
devem usar fields[data] V2. Localidades desconhecidas, tabelas isoladas e outras
formas de metadado requerem evidência futura; a solução conservadora se abstém.
O helper de assinatura compartilhado também reconhece cidade/em/data, além de
assinatura explícita; essa semântica preexistente não foi alterada neste item.
Não existe critério seguro para promover assinatura isolada a data documental.
Permanecem os problemas preexistentes de ACL em diretórios temporários e a
divergência de status documental de ADM-P1-002, somente registrados.

## Checklist e Git

ID: ADM-P0-002. Status atual TODO mantido por instrução explícita.
Status recomendado: VALIDATED, sujeito à validação humana; não houve auto-validação.
Evidência sugerida: resolução contextual V2 com raw/regra/localização; três datas
históricas corrigidas, cinco abstenções, legado/assinaturas preservados; 27 focados,
139 regressão e 575 suíte completa (570 aprovados/5 pulados), integridade intacta.
Nenhum outro ID foi iniciado ou atualizado.

Mensagem sugerida, sem criar commit:
`fix(admin): [ADM-P0-002] resolver data principal por contexto`.
