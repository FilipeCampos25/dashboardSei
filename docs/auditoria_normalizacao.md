# Auditoria do modulo de coleta e normalizacao

Data da auditoria: 2026-05-20.

Escopo: mapeamento estatico do fluxo atual para PT, ACT, Memorando e TED, sem alteracao de comportamento.

## Arquivos responsaveis

### Coleta dos documentos

- `backend/main.py`: ponto de entrada CLI; instancia `SEIScraper` e executa `run_full_flow`.
- `backend/app/rpa/scraping.py`: orquestrador principal da coleta no SEI. Define perfis de blocos internos, coleta preview de `PARCERIAS VIGENTES`, abre processos, busca documentos, extrai snapshots e finaliza os CSVs.
- `backend/app/rpa/sei/document_search.py`: utilitarios de busca no filtro `Pesquisar no Processo`. O codigo existe, mas o fluxo ativo chama `modo_busca_documental=tree_only`.
- `backend/app/rpa/sei/document_text_extractor.py`: extracao do snapshot do documento aberto, incluindo texto, tabelas, metadados e fallback por arquivo/OCR.
- `backend/app/integrations/transferegov_client.py`: cliente HTTP para consulta TED no Transferegov. Ha rota implementada, mas ela nao e chamada pelo `run_full_flow` atual.
- `backend/app/services/ted_api_processor.py`: transforma payload da API TED em snapshot/analysis canonicos.

### Classificacao documental

- `backend/app/documents/pt.py`: handler de PT, tracking operacional e chamada do normalizador PT.
- `backend/app/documents/cooperation_common.py`: handler comum para ACT, Memorando e TED, tracking operacional e manifestos gold enxutos.
- `backend/app/services/act_normalizer.py`: classificador semantico da familia de cooperacao e normalizador ACT.
- `backend/app/rpa/scraping.py`: validacao pre-salvamento de snapshot em `_validate_snapshot_for_document_type`; para PT aplica regra propria de minuta/documentacao, para ACT/Memorando/TED chama `classify_cooperation_snapshot`.

### Geracao dos CSVs

- `*_status_execucao_latest.csv`
  - PT: `PTDocumentHandler.finalize_run` em `backend/app/documents/pt.py`.
  - ACT/Memorando/TED: `CooperationDocumentHandler.finalize_run` em `backend/app/documents/cooperation_common.py`.
- `*_normalizado_latest.csv`
  - PT: `export_normalized_csv` em `backend/app/services/pt_normalizer.py`.
  - ACT: `export_normalized_csv` em `backend/app/services/act_normalizer.py`.
  - Memorando/TED: `_export_published_manifest` em `backend/app/documents/cooperation_common.py`.
- `dashboard_ready_latest.csv`: `export_dashboard_ready_csv` em `backend/app/services/dashboard_exporter.py`.
- `divergence_matrix_latest.csv`: tambem `export_dashboard_ready_csv` em `backend/app/services/dashboard_exporter.py`.

## Fluxo geral atual

`run_full_flow` abre o SEI, seleciona internos guiados, resolve um `InternalBlockProfile`, lista processos e abre cada processo. Os perfis atuais sao:

- `parcerias_vigentes`: descritor `PARCERIAS VIGENTES`, coleta preview e busca `pt`, `act`, `memorando`.
- `ted`: descritores de Termo de Execucao Descentralizada, busca `ted`.

Antes de buscar documentos no perfil `parcerias_vigentes`, o scraper coleta `parcerias_vigentes_latest.csv` a partir da tabela do interno, extraindo `processo`, `parceiro`, `vigencia`, `objeto` e `numero_act` das anotacoes.

No caminho principal, `_run_document_search_for_process` usa `tree_only` para todos os tipos: abre todas as pastas do processo, procura links no `ifrArvore`, pontua por `tree_match_terms`, abre candidatos em ordem de score e salva apenas quando o snapshot e canonico para o tipo solicitado. Candidatos nao canonicos podem ser retidos como silver, dependendo do tipo e do handler.

As rotinas de filtro `Pesquisar no Processo` existem e possuem aliases por tipo, limite de candidatos e fallback por arvore, mas nao sao acionadas pelo loop principal atual.

## PT

### Termos e local de busca

Configuracao em `backend/app/documents/pt.py`.

- `search_terms` configurados: `PLANO DE TRABALHO - PT`, `Plano de Trabalho - PT`, `Plano de Trabalho`, `PLANO DE TRABALHO PT`.
- `tree_match_terms` efetivamente usados no fluxo atual: `PLANO DE TRABALHO - PT`, `PLANO DE TRABALHO PT`, `PLANO DE TRABALHO`.
- Local: arvore do processo SEI (`ifrArvore`) dentro dos processos listados no interno `PARCERIAS VIGENTES`.
- Preview auxiliar: `parcerias_vigentes_latest.csv`.

### Selecao

- O candidato da arvore recebe score quando algum termo de PT aparece no texto do link.
- Candidatos com marcadores genericos de descarte pre-abertura podem ser pulados por `should_skip_candidate`.
- Apos abrir o documento, o snapshot so passa pela validacao se o blob contem `PLANO DE TRABALHO` e nao e pagina de pesquisa.
- PT canonico: contem marcador de plano de trabalho e nao e classificado como minuta/documentacao.

### Descarte e retencao

- Snapshot vazio ou pagina de pesquisa: rejeitado antes de salvar.
- Conteudo sem `PLANO DE TRABALHO`: rejeitado antes de salvar.
- Minuta/documentacao de PT: salvo como silver, com `doc_class=pt_minuta_documentacao`, `validation_status=related_but_not_canonical`, `publication_status=retained_silver`, `discard_reason=minuta_documentacao`.
- Registro de nao encontrado: entra no `pt_status_execucao_latest.csv` com status derivado do contexto (`not_found`, `filter_error`, `search_context_stagnation`, etc.).

### Campos extraidos

No snapshot JSON:

- `captured_at`, `document_type`, `processo`, `documento`.
- `snapshot.title`, `snapshot.url`, `snapshot.text`, `snapshot.tables`, `snapshot.extraction_mode`.
- `collection`: `found`, `found_in`, `search_term`, `results_count`, `chosen_documento`, `selection_reason`, `selection_detail`, `extraction_error`.
- `prazos`: resultado de `parse_prazos`, incluindo inicio/termino bruto e normalizado quando possivel.
- `analysis`: classificacao PT.

### Campos normalizados

`pt_auditoria_latest.csv`, `pt_normalizado_latest.csv` e `pt_normalizado_completo_latest.csv` usam:

- Identificacao: `captured_at`, `requested_type`, `resolved_document_type`, `processo`, `documento`.
- Conteudo: `parceiro`, `data_assinatura`, `datas_assinatura`, `vigencia_raw`, `vigencia_inicio`, `vigencia_fim`, `objeto`, `atribuições_raw`, `metas_raw`, `acoes_raw`.
- Prazo: `prazo_inicio_raw`, `prazo_inicio`, `prazo_fim_raw`, `prazo_fim`, `period_source`, `period_warning`.
- Auditoria: `selection_reason`, `classification_reason`, `validation_status`, `publication_status`, `snapshot_mode`, `preview_numero_act`, `normalization_status`, `captured_focus_fields`, `json_path`.

O normalizador cruza o snapshot com `parcerias_vigentes_latest.csv`. Para `parceiro`, `objeto` e `vigencia`, a previa e usada como fallback ou fonte preferencial em alguns casos.

### Status possiveis

- `validation_status`: `valid_for_requested_type`, `related_but_not_canonical`, alem de estados operacionais como `not_found`, `filter_error`, `search_context_stagnation`, `extraction_failure`.
- `publication_status`: `published_gold` quando e canonico e `normalization_status=completo_padronizado`; `retained_silver` nos demais casos.
- `normalization_status`: `completo_padronizado`, `parcial_padronizado`, `extraido_sem_padrao`, alem dos estados operacionais acima.
- `period_source`: `direct_label`, `derived_from_signature`, `unresolved_relative`, `unresolved_noise`, `missing_period`.

## ACT

### Termos e local de busca

Configuracao em `backend/app/documents/act.py`.

- `search_terms`/aliases configurados: variacoes de `ACORDO DE COOPERACAO TECNICA - ACT`, com e sem acento e com caixa mista.
- `tree_match_terms` efetivamente usados no fluxo atual: variacoes de `acordo de cooperacao tecnica`, `acordo de cooperacao tecnica - act` e `act`.
- Local: arvore do processo SEI (`ifrArvore`) dentro dos processos listados no interno `PARCERIAS VIGENTES`.

### Selecao

- Candidatos da arvore sao ranqueados por ocorrencia dos `tree_match_terms`.
- O pre-filtro `should_skip_candidate` descarta textos com `minuta`, `extrato`, `email`, `e-mail`, `anexo`, `termo aditivo`, `termo de adesao`, `documentacao`, `planilha`, `publicacao`.
- O snapshot aberto e classificado por `classify_cooperation_snapshot`.
- Para ACT, so e canonico se `doc_class=act_final`, possuir contexto interno CENSIPAM/Ministerio da Defesa/sistema correlato e o processo do documento estiver alinhado ou for referencia externa aceitavel.
- O normalizador ACT pode avaliar varios candidatos por processo em `output/candidates` e escolhe um canonico por `canonical_score`, tamanho do texto e tamanho do objeto.

### Descarte e retencao

- Classes relacionadas ou erradas ficam silver: `memorando`, `ted`, `extrato`, `minuta`, `termo_aditivo`, `termo_adesao`, `stub`, `email_outro`.
- `act_final` sem marcador interno vira `act_sem_marcador_interno`.
- `act_final` com processo materialmente divergente vira `processo_divergente_documento`.
- Se ha mais de um ACT valido, apenas o melhor vira gold; os demais recebem `descartado_por_desempate`.
- Sem candidato canonico, todos os registros do processo ficam `descartado_nao_canonico` e `retained_silver`.

### Campos extraidos

No snapshot JSON:

- Campos comuns do snapshot (`title`, `url`, `text`, `tables`, `extraction_mode`).
- `collection` com origem e detalhe de selecao.
- `analysis`: `doc_class`, `resolved_document_type`, `classification_reason`, `classification_priority`, `is_canonical_candidate`, `validation_status`, `publication_status`, `normalization_status`, `discard_reason`, `has_internal_context`, `process_alignment_status`, `document_processo`, `document_processos`.

Para ACT, snapshots de candidatos sao salvos em `backend/output/candidates/acordo_cooperacao_tecnica_*.json`; o canonico publicado tambem recebe alias `backend/output/acordo_cooperacao_tecnica_<processo>.json`.

### Campos normalizados

`act_classificacao_latest.csv` contem auditoria rica:

- Identificacao e classificacao: `requested_type`, `processo`, `numero_acordo`, `doc_class`, `resolved_document_type`, `is_canonical_candidate`, `validation_status`, `publication_status`, `normalization_status`, `discard_reason`, `classification_reason`, `canon_rejection_reason`.
- Conteudo: `data_inicio_vigencia`, `data_fim_vigencia`, `orgao_convenente`, `orgao_convenente_nome`, `orgao_convenente_sigla`, `orgao_intermediario`, `objeto`, `gestor_titular`, `gestor_substituto`, `unidade_responsavel`, `relatorio_encerramento`.
- Fontes e alertas: `field_source_numero_acordo`, `field_source_objeto`, `field_source_vigencia`, `field_source_gestao`, `vigencia_rule_*`, `vigencia_warning`, `validation_warning`.
- Processo e arquivo: `has_internal_context`, `process_alignment_status`, `document_processo`, `document_processos`, `snapshot_mode`, `text_chars`, `canonical_score`, `candidate_json_path`, `json_path`.

`act_normalizado_latest.csv` publica apenas o canonico gold com:

- `numero_acordo`, `processo`, `data_inicio_vigencia`, `data_fim_vigencia`, `orgao_convenente`, `orgao_convenente_nome`, `orgao_convenente_sigla`, `orgao_intermediario`, `objeto`, `gestor_titular`, `gestor_substituto`, `unidade_responsavel`, `classificacao`, `relatorio_encerramento`.

### Status possiveis

- `doc_class`: `act_final`, `memorando`, `ted`, `extrato`, `minuta`, `termo_aditivo`, `termo_adesao`, `stub`, `email_outro`.
- `validation_status`: `valid_for_requested_type`, `related_but_not_requested`, `rejected_snapshot`, estados operacionais como `not_found`, `filter_error`, `search_context_stagnation`, `extraction_failure`.
- `publication_status`: `published_gold`, `retained_silver`.
- `normalization_status`: `publicado_canonico`, `classificado_canonico`, `descartado_semantico`, `descartado_nao_canonico`, `descartado_por_desempate`, estados operacionais.

## Documento administrativo / Memorando

### Termos e local de busca

Configuracao em `backend/app/documents/memorando.py`.

- `search_terms` e aliases de filtro configurados: `memorando`, `memo`, `ofício`, `oficio`, `despacho`, `encaminhamento`, `informação técnica`, `informacao tecnica`, `nota técnica`, `nota tecnica`, `solicitação`, `solicitacao`.
- `tree_match_terms` usam o mesmo conjunto de termos para reduzir `not_found` quando o documento administrativo existe com outro nome.
- Local: arvore do processo SEI (`ifrArvore`) dentro dos processos listados no interno `PARCERIAS VIGENTES`.

### Selecao

- Candidato da arvore precisa conter pelo menos um dos termos administrativos configurados.
- Snapshot e classificado por `classify_cooperation_snapshot`.
- Canonico quando `doc_class` resolve para uma das classes administrativas: `memorando`, `oficio`, `despacho`, `informacao_tecnica`, `nota_tecnica`, `documento_administrativo_relacionado`.

### Descarte e retencao

- Snapshot vazio, pagina de pesquisa ou email/stub pode ser rejeitado ou retido como silver conforme passa pela validacao.
- Classes fora da familia administrativa recebem `related_but_not_requested` ou `rejected_snapshot`, com `publication_status=retained_silver`.
- Ha normalizacao propria para documentos administrativos; `memorando_normalizado_latest.csv` passa a ser apenas visao filtrada dos registros classificados como `memorando`.

### Campos extraidos

Mesmo conjunto comum de snapshot e `analysis` da familia de cooperacao:

- `title`, `url`, `text`, `tables`, `extraction_mode`.
- `collection`.
- `analysis` com `doc_class`, `resolved_document_type`, `classification_reason`, `validation_status`, `publication_status`, `normalization_status`, `discard_reason`.

### Campos normalizados

`documento_administrativo_normalizado_latest.csv` publica documentos administrativos encontrados com:

- Identificacao/classificacao: `captured_at`, `requested_type`, `processo`, `documento`, `resolved_document_type`, `funcao_administrativa`.
- Campos extraidos: `origem`, `destino`, `data`, `data_assinatura`, `datas_assinatura`, `assunto`, `resumo`, `acao_solicitada`, `prazo`, `documentos_mencionados`.
- Auditoria: `selection_reason`, `classification_reason`, `validation_status`, `publication_status`, `snapshot_mode`, `json_path`.

`memorando_normalizado_latest.csv` contem somente a visao filtrada de `documento_administrativo_normalizado_latest.csv` para `doc_class=memorando`.

### Status possiveis

Iguais aos da familia de cooperacao, com gold quando `doc_class` pertence a familia administrativa:

- `validation_status`: `valid_for_requested_type`, `related_but_not_requested`, `rejected_snapshot`, estados operacionais.
- `publication_status`: `published_gold`, `retained_silver`.
- `normalization_status`: `publicado_canonico`, `descartado_semantico`, estados operacionais.

## TED

### Termos e local de busca

Configuracao em `backend/app/documents/ted.py`.

- `search_terms`/aliases configurados: `Termo de Execucao Descentralizada`, versoes com acento, `TED - Termo de Execucao Descentralizada`, versoes com acento e `TED`.
- `tree_match_terms` efetivamente usados no fluxo atual: `ted - termo de execucao descentralizada`, versao acentuada, `termo de execucao descentralizada`, versao acentuada.
- Local atual no `run_full_flow`: arvore do processo SEI (`ifrArvore`) no perfil de interno TED.
- Rota alternativa implementada: API Transferegov via `consultar_ted`, usando `numeroProcesso`, `numeroInstrumento`, `anoInstrumento` e `anoProcesso`. Esta rota depende de numero de instrumento em previa (`numero_ted`, `numero_instrumento_ted`, `numero_instrumento`, `instrumento_ted`), mas nao e chamada pelo loop principal atual.

### Selecao

- Na arvore, candidato precisa pontuar por termo TED.
- O snapshot aberto e classificado por `classify_cooperation_snapshot`.
- Canonico apenas quando `doc_class=ted`.
- Na rota API, `build_ted_api_analysis` marca diretamente como `doc_class=ted`, `valid_for_requested_type`, `published_gold`, `publicado_canonico`.

### Descarte e retencao

- Pela arvore, qualquer classe diferente de TED fica silver ou rejeitada.
- Pela API, os descartes previstos sao: processo invalido, ausencia de numero de instrumento, sem resultado na API ou erro HTTP/JSON.
- Nao ha normalizacao rica para TED; ha manifesto gold. Campos especificos de TED aparecem no dashboard por leitura do `api_payload` quando o JSON veio da API.

### Campos extraidos

Pela arvore:

- Campos comuns de snapshot e `analysis` da familia de cooperacao.

Pela API:

- Snapshot sintetico com `extraction_mode=api`, `source=transferegov_api`, `title=TED via API`, `url` da API, `text` sintetico, `api_payload` processado e `api_raw` bruto.
- `api_payload`: `numero_processo`, `objeto`, `valor_global`, `situacao`, `uf`, `itens`.

### Campos normalizados

`ted_normalizado_latest.csv` e manifesto de publicados:

- `captured_at`, `requested_type`, `processo`, `documento`, `resolved_document_type`, `selection_reason`, `classification_reason`, `validation_status`, `publication_status`, `snapshot_mode`, `json_path`.

No `dashboard_ready_latest.csv`, quando `ted_json_path` existe, sao derivados do JSON:

- `ted_objeto`, `ted_valor_global`, `ted_situacao`, `ted_uf`.

### Status possiveis

- `validation_status`: `valid_for_requested_type`, `related_but_not_requested`, `rejected_snapshot`, `invalid_processo_number`, `skipped_no_instrument_number`, `no_results_in_api`, alem de estados operacionais.
- `publication_status`: `published_gold`, `retained_silver`.
- `normalization_status`: `publicado_canonico`, `descartado_semantico`, estados operacionais.

## Consolidacao dashboard

`export_dashboard_ready_csv` parte de `parcerias_vigentes_latest.csv` e acrescenta processos gold de `ted_normalizado_latest.csv` que nao aparecem na previa. A consolidacao principal continua orientada pela previa de parcerias, mas TEDs canonicos coletados em universo proprio deixam de ficar invisiveis.

Antes do cruzamento, a chave `processo` e normalizada para o formato `00000.000000/0000-00`: remove espacos, padroniza barra/hifen e remascara valores com 17 digitos. Chaves fora desse formato permanecem rastreaveis e entram em `normalization_issues` como `processo_invalido`.

Para cada processo consolidado, ele cruza:

- `pt_auditoria_latest.csv`.
- `act_classificacao_latest.csv`.
- `act_status_execucao_latest.csv`.
- `memorando_normalizado_latest.csv`.
- `ted_normalizado_latest.csv`, considerando TED gold somente quando `publication_status=published_gold`, `validation_status=valid_for_requested_type` e `json_path` existe.
- `ted_status_execucao_latest.csv`.

O arquivo `dashboard_ready_latest.csv` calcula presenca gold, caminhos JSON, qualidade por tipo, melhores valores consolidados (`best_numero_acordo`, `best_parceiro`, `best_vigencia`, `best_data_assinatura`, `best_datas_assinatura`, `best_objeto`) e `normalization_issues`.

O arquivo `divergence_matrix_latest.csv` e um recorte de auditoria de divergencias, com qualidade PT/ACT/TED, `ted_gold`, `ted_json_path`, universo de origem (`parcerias_vigentes` ou `ted_normalizado`), chave de join canonica, validade da chave, tentativas ACT, campos faltantes, comparacao previa x ACT/PT e fontes escolhidas.

Ponto importante: TED com `validation_status=related_but_not_requested` e/ou `publication_status=retained_silver` nao e tratado como gold, mesmo que possua JSON. Quando houver processo correspondente no dashboard, ele aparece como TED ignorado nas issues; quando estiver fora da previa, nao cria linha nova.

## Pontos fortes

- Separacao clara entre especificacao documental (`DocumentTypeSpec`), handler de persistencia/tracking e normalizadores.
- Rastreabilidade boa: snapshots JSON preservam texto, tabelas, contexto de selecao e analysis.
- ACT possui auditoria rica (`act_classificacao_latest.csv`) e mantem candidatos silver antes de publicar um canonico.
- PT publica auditoria completa e separa gold de silver.
- Existe reparo de mojibake em pontos criticos, reduzindo impacto de termos acentuados quebrados no fonte ou no SEI.
- Dashboard usa fontes e niveis de confianca para valores consolidados, em vez de sobrescrever tudo cegamente.

## Pontos frageis

- O fluxo ativo ignora o filtro `Pesquisar no Processo` e usa somente arvore. Isso reduz cobertura quando o documento nao esta visivel/expandido na arvore ou quando o ranking pelo texto do link e fraco.
- A normalizacao rica existe apenas para PT e ACT. Memorando e TED geram manifesto gold, sem extracao estruturada comparavel.
- A rota TED via API esta implementada mas nao conectada ao `run_full_flow`; na pratica, TED tende a depender da arvore.
- `dashboard_ready_latest.csv` e ancorado em `parcerias_vigentes_latest.csv`, mas tambem inclui TED gold fora desse universo com `source_universe=ted_normalizado` na matriz de divergencia.
- Muitos criterios dependem de heuristicas textuais rigidas e janelas fixas de caracteres.
- ACT exige marcadores internos CENSIPAM/Ministerio da Defesa; isso protege contra falso positivo, mas pode descartar ACT legitimo com redacao diferente.
- A deduplicacao e escolha canonica do ACT sao boas, mas a logica de score mistura sinais de conteudo, rotulo e penalidades, o que dificulta explicar por que um candidato venceu.

## Bugs provaveis

- `scraping._run_document_search_for_process` sempre registra `modo_busca_documental=tree_only`; as rotinas de filtro e seus `search_terms`/`filter_type_aliases` estao praticamente mortas no caminho principal.
- `_process_ted_via_api` nao tem chamada no fluxo principal. Ha teste chamando o metodo diretamente, mas nenhum uso real no `run_full_flow`.
- Em `_score_tree_candidate`, `self._normalize_text` retorna texto em maiusculas, mas varias penalidades sao comparadas com marcadores em minusculas (`email`, `minuta`, `termo aditivo`, marcadores ACT de `TREE_PENALTY_MARKERS`). Essas penalidades provavelmente nao sao aplicadas no score da arvore. O pre-descarte posterior ainda funciona, mas o ranking pode ficar distorcido.
- `document_utils.SKIP_CANDIDATE_MARKERS` descarta `documentacao` para todos os tipos; para PT isso pode pular documentos chamados `Documentacao - Minutas ACT e Plano de Trabalho` antes da classificacao silver, reduzindo rastreabilidade de minutas/documentacao.
- `CooperationDocumentHandler.finalize_run` so chama o normalizador rico quando `export_act_normalized=True`; Memorando e TED perdem campos estruturados mesmo quando o snapshot contem texto suficiente.
- `derive_search_outcome_status` mapeia quase tudo sem status explicito para `not_found`; alguns erros de arvore (`tree_search_error`, `tree_open_error`) podem ficar semanticamente pobres no CSV final.
- Ha termos mojibake persistidos no codigo fonte (`ExecuÃ§Ã£o`, `CooperaÃ§Ã£o`). O reparo posterior ajuda, mas comparacoes que nao passam por sanitizacao podem falhar.

## Recomendacoes de refatoracao

- Tornar explicito o modo de busca por tipo: `tree_only`, `filter_then_tree`, `api_then_tree`. Hoje o codigo implementa varios modos, mas o caminho ativo mascara isso.
- Conectar TED a uma estrategia clara: tentar API quando houver numero de instrumento, registrar motivo quando nao houver, e so entao cair para arvore.
- Unificar normalizacao de texto em uma unica funcao com contrato claro de caixa (`lower` ou `upper`) e usar isso tambem nas penalidades de score.
- Separar triagem pre-abertura por tipo documental. Para PT, `documentacao` pode ser candidato silver relevante; para ACT, faz sentido descartar cedo.
- Criar normalizadores minimos para Memorando e TED com campos proprios, mesmo que inicialmente sejam poucos (`numero`, `partes`, `objeto`, `vigencia` para Memorando; `objeto`, `valor`, `situacao`, `uf`, `numero_instrumento` para TED).
- Avaliar se Memorando tambem deve ampliar o universo consolidado, como TED gold ja faz, ou se deve permanecer apenas como enriquecimento da previa de parcerias.
- Extrair as regras de status para enums/constantes compartilhadas, reduzindo divergencia entre handlers, normalizadores e dashboard.
- Adicionar testes focados no fluxo ativo `tree_only`, principalmente para ranking com penalidades, pre-descarte por tipo e TED API nao acionada.
