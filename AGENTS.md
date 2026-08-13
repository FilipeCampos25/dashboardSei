# AGENTS.md

## Propósito

Este arquivo define regras gerais de trabalho para agentes de código neste repositório.

Use estas instruções como padrão para qualquer tarefa, salvo quando o prompt atual trouxer uma regra mais específica ou mais restritiva.

O objetivo é aumentar qualidade, rastreabilidade e segurança sem transformar mudanças pequenas em burocracia desnecessária.

---

## 1. Ordem de precedência e princípio geral

Ao executar uma tarefa, siga esta ordem de autoridade:

1. instruções explícitas do prompt atual;
2. regras deste `AGENTS.md`;
3. documentação técnica e regras de negócio do repositório;
4. comportamento comprovado pelo código e pelos testes existentes.

Uma instrução mais específica pode restringir uma regra geral, mas não deve ser interpretada como autorização para violar segurança, expor credenciais ou executar ações destrutivas sem autorização explícita.

Antes de alterar código:

1. entenda o objetivo real da tarefa;
2. inspecione o código existente e os arquivos relacionados;
3. confirme o comportamento atual quando isso for relevante;
4. identifique integrações, dependências, contratos e possíveis efeitos colaterais;
5. só então altere o código.

Não implemente com base apenas no nome de um arquivo, função, issue ou suposição.

Quando houver incerteza material, investigue antes de modificar.

---

## 2. Escopo da alteração

Faça a menor alteração coerente capaz de resolver o problema.

- Não refatore código não relacionado apenas porque ele poderia ser melhorado.
- Não altere arquitetura, bibliotecas, contratos, formatos de dados ou regras de negócio sem necessidade clara.
- Não aproveite uma tarefa pequena para fazer uma limpeza ampla.
- Preserve comportamento existente que não faça parte do problema.
- Mudanças pequenas e inseparáveis podem ser tratadas como uma única unidade lógica.
- Não é necessário criar uma alteração, teste ou commit separado para cada mudança textual mínima.

Se durante a investigação forem encontrados outros problemas:

- registre-os no relatório final;
- não os corrija automaticamente, salvo se forem necessários para concluir a tarefa atual com segurança.

Se uma correção incidental afetar outra tarefa ou item da checklist, registre o efeito, mas não conclua automaticamente o outro item.

---

## 3. Diagnóstico antes da correção

Para bugs, regressões ou comportamentos inesperados:

1. localize a origem provável;
2. reproduza ou confirme o problema quando possível;
3. diferencie causa raiz de sintoma;
4. verifique se já existe teste cobrindo o comportamento;
5. corrija a causa raiz preferencialmente.

Não modifique código apenas porque uma solução "parece funcionar".

Quando o diagnóstico não puder ser confirmado, deixe explícito o que é:

- fato observado;
- hipótese;
- inferência;
- limitação da investigação.

Não transforme incerteza secundária em bloqueio geral quando ainda houver trabalho seguro e independente que possa ser executado.

---

## 4. Regras de negócio, contratos e contexto do projeto

Antes de alterar comportamento:

- procure documentação do projeto;
- procure testes existentes;
- procure tipos, schemas, interfaces, DTOs, modelos, migrations, configurações e contratos relacionados;
- procure chamadas diretas e indiretas da função alterada;
- identifique consumidores do comportamento;
- identifique regras de negócio relevantes;
- confirme se existem arquivos de arquitetura, desenvolvimento, design, orquestração ou ADRs aplicáveis.

Não invente regra de negócio ausente.

Se duas partes do projeto parecerem contraditórias, investigue e exponha a divergência em vez de escolher silenciosamente uma interpretação.

Ao alterar comportamento existente:

- identifique consumidores;
- preserve interfaces quando possível;
- documente breaking changes inevitáveis;
- prefira migração gradual quando o risco for significativo;
- não remova código legado antes de confirmar que ele não possui consumidores relevantes.

---

## 5. Pesquisa e documentação externa

Quando a tarefa depender de informação externa, comportamento de biblioteca, framework, API, linguagem, ferramenta ou padrão que possa ter mudado:

- pesquise documentação atual quando houver acesso à web;
- priorize documentação oficial, especificações, repositórios oficiais e fontes primárias;
- confirme versão e contexto antes de aplicar uma solução;
- não trate memória do modelo como fonte suficiente para informação sensível a versão.

Não faça pesquisa web apenas por ritual quando o código local já fornece evidência suficiente para uma alteração trivial.

Se uma decisão importante for baseada em documentação externa, registre no relatório final qual comportamento foi verificado.

---

## 6. Qualidade de implementação

Prefira código:

- simples;
- legível;
- explícito;
- testável;
- consistente com o estilo existente;
- proporcional ao problema.

Evite abstrações prematuras e arquiteturas desnecessárias.

Não introduza complexidade apenas para demonstrar sofisticação técnica.

Reutilize padrões já estabelecidos no projeto quando eles forem adequados.

Não substitua uma implementação funcional inteira quando uma correção localizada resolve o problema com menor risco.

---

## 7. Dependências

Antes de adicionar ou atualizar uma dependência:

1. verifique se o projeto já possui solução equivalente;
2. confirme a necessidade;
3. considere compatibilidade com as versões existentes;
4. consulte documentação oficial quando necessário;
5. evite adicionar dependência para resolver algo simples que o próprio projeto já consegue fazer.

Nunca altere uma versão importante de biblioteca como efeito colateral silencioso de outra tarefa.

---

## 8. Banco de dados e persistência

Mudanças em schema, migrations ou persistência exigem atenção especial.

- Preserve dados existentes por padrão.
- Não remova colunas, tabelas ou dados sem autorização explícita.
- Evite migrations destrutivas.
- Considere compatibilidade entre código antigo e schema novo quando aplicável.
- Teste leitura e escrita relevantes.
- Documente qualquer migration necessária.

---

## 9. Tratamento de erros

Não silencie erros apenas para fazer testes passarem.

Evite:

- `except` excessivamente amplo sem justificativa;
- fallback que transforma erro em sucesso aparente;
- valores default que escondem ausência de dado importante;
- remoção de validações sem entender sua função;
- tratamento que converta falha técnica em sucesso ou decisão semântica sem evidência.

Erros esperados devem ser tratados de forma explícita e observável.

---

## 10. Segurança

Nunca exponha:

- senhas;
- tokens;
- cookies;
- chaves de API;
- credenciais;
- conteúdo real de `.env`;
- dados sensíveis desnecessários.

Não copie segredos para:

- logs;
- testes;
- fixtures;
- documentação;
- issues;
- mensagens de commit;
- relatórios.

Não execute ações destrutivas, acesso a produção, deploy, migrations destrutivas, chamadas externas sensíveis ou operações irreversíveis sem autorização explícita.

Quando dados reais forem necessários para testes, prefira fixtures sanitizadas, mocks ou dados sintéticos.

---

## 11. Testes

Toda alteração funcional deve ser validada de forma proporcional ao risco.

### 11.1 Correções de bug

Quando possível:

1. crie ou identifique um teste que reproduza o problema;
2. confirme que ele falha pelo motivo esperado;
3. implemente a correção;
4. confirme que o teste passa.

### 11.2 Novas funcionalidades

Cubra pelo menos:

- caminho principal;
- casos de erro relevantes;
- limites ou estados importantes;
- compatibilidade com comportamento existente quando aplicável.

### 11.3 Ordem de execução

Priorize:

1. testes focados na alteração;
2. testes do módulo ou componente afetado;
3. regressão mais ampla quando o risco justificar.

Não declare testes como aprovados se eles não foram executados.

Se um teste falhar:

- investigue se a falha foi causada pela alteração;
- diferencie falha nova de falha preexistente ou ambiental;
- registre claramente a situação.

Testes manuais são válidos quando apropriados, mas não substituem automaticamente testes automatizados quando estes forem viáveis e importantes.

Não crie mocks que testem apenas o próprio mock.

---

## 12. Revisão obrigatória antes de concluir

Antes de considerar a tarefa finalizada:

1. revise o diff completo;
2. procure alterações acidentais;
3. procure código morto, logs temporários, prints de debug e comentários provisórios;
4. confira imports, nomes, tipos e tratamento de erros;
5. confira possíveis regressões;
6. confira se os testes realmente cobrem o comportamento alterado;
7. confira se a documentação necessária foi atualizada;
8. execute `git diff --check` quando Git estiver disponível;
9. confirme que nenhum segredo ou dado sensível foi introduzido;
10. confirme que não existem mudanças fora do escopo.

Nunca trate a primeira implementação como automaticamente correta.

Quando encontrar um problema na própria implementação:

1. corrija-o;
2. repita os testes relevantes;
3. revise novamente o diff afetado.

---

## 13. Documentação

Documente mudanças relevantes enquanto trabalha.

Atualize documentação quando a alteração afetar:

- comportamento público;
- regras de negócio;
- API;
- configuração;
- instalação;
- execução;
- arquitetura;
- fluxo de dados;
- banco de dados;
- formato de arquivos;
- processo de desenvolvimento;
- decisões técnicas relevantes.

Use comentários no código para explicar **por que** algo existe quando isso não for evidente.

Evite comentários que apenas repetem o código.

Não crie documentação artificial para alterações triviais que não mudem comportamento, contrato ou entendimento do sistema.

Quando existir documentação de progresso, changelog, checklist, ADR, README técnico ou documento equivalente no projeto, mantenha-o coerente com a alteração realizada.

---

## 14. Git e rastreabilidade

Trate cada tarefa como uma unidade lógica rastreável.

- Mantenha o diff focado.
- Evite misturar mudanças independentes.
- Não reescreva histórico publicado.
- Não execute `git reset --hard`, force-push, exclusões destrutivas ou operações equivalentes sem autorização explícita.
- Não faça commit ou push automaticamente salvo quando o prompt autorizar.
- Quando não houver autorização para commit, sugira uma mensagem de commit adequada no relatório final.

Um commit deve representar uma unidade lógica compreensível e reversível.

Não é necessário criar um commit por linha, variável ou alteração textual mínima.

Antes de propor ou criar um commit, confirme que o conjunto de alterações forma uma unidade coerente e que os testes relevantes foram executados.

---

## 15. Protocolo da checklist operacional

Esta seção se aplica quando a tarefa estiver vinculada a uma checklist com IDs rastreáveis.

### 15.1 Escopo por ID

Quando uma tarefa possuir ID da checklist:

- trabalhe somente no ID explicitamente autorizado pelo prompt;
- não avance automaticamente para o próximo item;
- não altere título, prioridade, dependências ou escopo do item sem autorização;
- atualize somente os campos operacionais do item atual;
- não marque outros itens como concluídos apenas porque foram parcialmente afetados.

Caso a implementação resolva incidentalmente outro item:

- registre isso no relatório;
- mantenha o outro item inalterado até ser validado explicitamente.

### 15.2 Estados permitidos

Fluxo padrão:

`TODO → READY → IN_PROGRESS → IMPLEMENTED → VALIDATED`

Estados:

- `TODO`: ainda não iniciado ou sem precondições confirmadas.
- `READY`: diagnóstico e precondições permitem iniciar.
- `IN_PROGRESS`: investigação ou implementação iniciada.
- `BLOCKED`: existe impedimento objetivo que impossibilita continuar com segurança.
- `IMPLEMENTED`: código concluído e testes relevantes executados, mas ainda existe validação pendente.
- `VALIDATED`: implementação, testes, diff e critérios de aceite foram verificados com sucesso.
- `REJECTED`: solução implementada foi considerada incorreta ou inadequada.
- `SUPERSEDED`: item foi substituído formalmente por outro ID.

### 15.3 Atualização ao iniciar

Ao iniciar um item autorizado:

1. marque somente esse item como `IN_PROGRESS`;
2. não altere outros itens.

### 15.4 Atualização ao concluir

Ao concluir:

- registre evidência de implementação;
- registre arquivos modificados;
- registre resumo do diff;
- registre testes executados;
- registre resultados dos testes;
- registre limitações ou riscos residuais;
- registre commit, se houver;
- atualize o status conforme as regras de validação.

---

## 16. Validação automática de item da checklist

Por padrão, uma implementação termina em `IMPLEMENTED`.

O agente pode promover o item diretamente para `VALIDATED` somente quando o prompt atual contiver autorização explícita para auto-validação e **todos** os seguintes critérios forem satisfeitos:

1. o diagnóstico foi confirmado;
2. o critério de aceite do item foi satisfeito;
3. os testes focados passaram;
4. os testes de regressão exigidos pelo item passaram;
5. nenhum teste crítico foi omitido;
6. nenhuma falha nova permaneceu sem explicação;
7. o diff completo foi revisado;
8. não existem alterações acidentais ou fora do escopo;
9. snapshots e outputs protegidos permaneceram inalterados;
10. nenhuma credencial ou dado sensível foi exposto;
11. não houve quebra de compatibilidade não documentada;
12. `git diff --check` não apresenta erro;
13. não existem prints, logs ou debug temporários introduzidos pela alteração;
14. a documentação exigida pelo item está coerente com a implementação.

Se algum requisito não puder ser confirmado, deixe o item como `IMPLEMENTED` ou `BLOCKED` e explique objetivamente o motivo.

---

## 17. Uso de BLOCKED

Não use `BLOCKED` apenas porque:

- existe alguma incerteza secundária;
- um teste não relacionado falhou;
- a suíte completa possui falha preexistente;
- existe melhoria possível fora do escopo;
- outro item relacionado ainda está `TODO`;
- uma dependência da checklist está `IMPLEMENTED` mas tecnicamente disponível.

Use `BLOCKED` somente quando existir impedimento objetivo para concluir o item atual, como:

- decisão de negócio indispensável ausente;
- fixture ou evidência necessária inexistente;
- dependência técnica realmente não implementada;
- risco de segurança que impede prosseguir;
- impossibilidade de testar um comportamento crítico;
- contradição de requisitos que não pode ser resolvida pelo código ou documentação existente.

Quando apenas uma parte estiver bloqueada, continue executando todo o trabalho seguro e independente possível.

---

## 18. Baseline e artefatos protegidos

Durante correções offline, trate como somente leitura:

- snapshots da coleta congelada;
- CSVs da baseline;
- JSONs da baseline;
- logs da coleta congelada;
- bancos usados apenas como evidência;
- artefatos bronze, silver e gold originais;
- qualquer outro arquivo explicitamente marcado como baseline, snapshot ou evidência.

É permitido:

- copiá-los para diretório temporário;
- carregá-los em testes;
- reprocessá-los para uma saída temporária ou V2;
- calcular hashes e métricas.

Não é permitido:

- editar a baseline;
- sobrescrever arquivos originais;
- alterar valores manualmente para fazer testes passarem.

---

## 19. Comunicação e tomada de decisão

Não esconda incertezas.

Quando houver mais de uma solução razoável:

- compare brevemente os trade-offs;
- escolha a alternativa mais simples que respeite os requisitos;
- registre decisões importantes.

Se uma decisão exigir conhecimento de negócio que não existe no código, testes ou documentação, não invente a resposta.

Quando possível, continue com trabalho seguro e isolado; bloqueie apenas a parte que realmente depende dessa decisão.

---

## 20. Relatório final obrigatório

Ao concluir uma tarefa de implementação, apresente de forma curta e objetiva:

### Diagnóstico
O que estava acontecendo e qual causa foi confirmada.

### Alterações
Arquivos modificados e comportamento alterado.

### Testes
Comandos e testes executados, com seus resultados.

### Documentação
O que foi atualizado ou por que nenhuma atualização foi necessária.

### Revisão
Principais verificações feitas no diff.

### Checklist
Quando houver item rastreável, informe ID, status final e evidências registradas.

### Riscos ou limitações
Pendências, hipóteses, falhas preexistentes ou pontos não validados.

### Git
Sugestão de mensagem de commit, salvo quando o próprio prompt já definir uma.

---

## 21. Critério de conclusão

Uma tarefa só deve ser considerada concluída quando, proporcionalmente ao seu risco:

- o diagnóstico estiver suficientemente confirmado;
- a implementação estiver coerente com o objetivo;
- o diff estiver focado;
- os testes relevantes tiverem sido executados;
- falhas tiverem sido investigadas;
- a documentação necessária estiver atualizada;
- o diff tiver sido revisado;
- limitações tiverem sido declaradas;
- o status da checklist, quando aplicável, refletir o nível real de validação.

---

## 22. Restrições finais

Não:

- altere arquivos fora do escopo sem necessidade;
- masque falhas de teste;
- edite outputs gerados manualmente apenas para fazer resultado bater;
- crie mocks que testem apenas o próprio mock;
- remova validações sem diagnóstico;
- adicione fallback silencioso para esconder erro;
- substitua implementação funcional inteira quando uma correção localizada resolve;
- mude regra de negócio por preferência técnica;
- adicione dependências desnecessárias;
- deixe logs ou debug temporários;
- afirme que algo foi validado sem evidência;
- avance automaticamente para outra tarefa sem autorização quando houver checklist rastreável.

**Código escrito não é automaticamente código validado.**
