# Task para Claude Code — Correção conceitual e técnica de α₁

**Contexto:** Este projeto implementa o ICSEIOM (Índice de Custo Socioambiental
Evitado por Incidentes com Óleo no Mar) para a LGAF / IEAPM / Marinha do Brasil.
O sistema web está em `sistema_icseiom_web/`, documentação em
`../ICSEIOM_proposta.docx` e apresentações em `../dinamica 2.*.pptx`.

**Por que esta task existe:** A definição e a regra de cálculo do α₁
(componente de multa ambiental) foram implementadas com lógica invertida em
relação ao modelo de Lucro Social do livro "LUCRO SOCIAL: FERRAMENTA DE
TRANSPARÊNCIA" e ao modelo Sefluq que o livro usa como referência. Precisa
ser corrigido em cadeia: código, banco, texto, apresentações e `CLAUDE.md`.

---

## 1. Diagnóstico da lógica antiga (errada)

No arquivo `app/calc.py`, dentro de `calcular_icseiom()`:

```python
if foi_poluente:
    a1 = 0.0
```

Isto zera α₁ quando o óleo foi poluente. A interpretação implícita era
"multa evitada = laudo negativo = lucro". Está invertido.

## 2. Lógica nova (correta)

**α₁ tem dois subcomponentes que se SOMAM, e o ramo depende do resultado
do laudo:**

### Ramo A — Laudo atesta óleo poluente (`foi_poluente = True`)

```
α₁⁺ = f_apropriacao × valor_multa_aplicada
    + custo_analise_externa_evitada
```

- `f_apropriacao`: fração da multa imputável ao trabalho do laboratório
  (0 ≤ f ≤ 1). Placeholder `0,30`. Precisa validação Delphi. A justificativa
  é que parte do valor da multa é lucro social do próprio IBAMA (fiscalização
  e processo administrativo), não só do laboratório.
- `valor_multa_aplicada`: vem do IBAMA SICAFI para o evento específico.
  Enquanto não houver integração real, usar o histórico municipal como
  aproximação (tabela `alpha1_multa_ambiental.valor_rs`).
- `custo_analise_externa_evitada`: valor médio de mercado para análise
  cromatográfica forense equivalente, contratada de laboratório externo.
  Placeholder `R$ 35.000,00` por evento (precisa cotação real de
  laboratórios credenciados — Petrobras Cenpes, USP IO, UFRJ LADETEC, etc.).

### Ramo B — Laudo atesta óleo não-poluente (`foi_poluente = False`)

```
α₁⁻ = P(autuacao_erronea) × E[valor_multa_area] × (1 − recuperacao_defesa)
    + custo_analise_externa_evitada
```

- `P(autuacao_erronea)`: probabilidade histórica de o IBAMA autuar
  indevidamente sem laudo técnico, na área/tipo de evento. Placeholder
  `0,25`. Precisa série histórica SICAFI (quantas autuações foram revertidas
  administrativamente em recursos).
- `E[valor_multa_area]`: valor esperado da multa na área, vindo do histórico
  municipal (tabela `alpha1_multa_ambiental.valor_rs` / `n_autos`).
- `recuperacao_defesa`: fração do valor que a empresa inocente recuperaria
  em defesa administrativa (o ganho real evitado é só a parte que ela NÃO
  recuperaria naturalmente). Placeholder `0,60` → sobra 40% como lucro
  social genuíno.
- `custo_analise_externa_evitada`: idem ao ramo A.

### Regra de exclusão mútua

Um evento é sempre OU ramo A OU ramo B. Nunca os dois ao mesmo tempo,
sob pena de dupla contagem. No código atual, o campo `eventos.foi_poluente`
já garante isso.

---

## 3. O que editar no código

### 3.1 `app/calc.py`

Substituir o trecho de α₁ por algo como:

```python
# Parâmetros apropriados (idealmente lidos de parametros)
F_APROPRIACAO = 0.30
P_AUTUACAO_ERRONEA = 0.25
RECUPERACAO_DEFESA = 0.60
CUSTO_ANALISE_EXTERNA = 35_000.0  # R$ por evento

multa_hist = soma("alpha1_multa_ambiental", "valor_rs")

if foi_poluente:
    a1 = F_APROPRIACAO * multa_hist + CUSTO_ANALISE_EXTERNA
else:
    a1 = (P_AUTUACAO_ERRONEA * multa_hist * (1 - RECUPERACAO_DEFESA)
          + CUSTO_ANALISE_EXTERNA)
```

Idealmente, os quatro parâmetros vêm da tabela `parametros` (não hardcoded).
Ver seção 3.2.

### 3.2 `scripts/07_carregar_parametros.py`

Adicionar ao dicionário `PARAMETROS`:

```python
("f_apropriacao_alpha1",     0.30,  "fração",    "Fração da multa imputável ao LGAF"),
("p_autuacao_erronea",       0.25,  "prob",      "P de autuação errônea sem laudo"),
("recuperacao_defesa",       0.60,  "fração",    "Fração recuperável via defesa administrativa"),
("custo_analise_externa_rs", 35000, "R$/evento", "Custo médio de análise cromatográfica forense externa"),
```

E então em `calc.py` ler esses valores da tabela `parametros` em vez de
usar constantes Python.

### 3.3 `app/templates/evento.html`

Na tabela de decomposição, substituir a linha:

```
α₁ multa ambiental         R$ ...
```

Por duas linhas que decompõem o α₁ aplicado, por exemplo:

```
α₁ multa (parcela LGAF)    R$ ... (= f × valor aplicado, ou P × E × (1-r) × valor histórico)
α₁ análise externa evitada R$ 35.000,00
```

Isso exige persistir os dois subcomponentes. Opção A: adicionar colunas
`alpha1a_rs`, `alpha1b_rs` na tabela `resultados`. Opção B: manter só
`alpha1_rs` e recalcular na exibição. Prefira Opção A para auditoria.

### 3.4 Migração de schema

Se optar pela Opção A, criar um script
`scripts/08_migrate_alpha1_split.py` que:

1. `ALTER TABLE resultados ADD COLUMN alpha1_parcela_multa_rs REAL DEFAULT 0`
2. `ALTER TABLE resultados ADD COLUMN alpha1_analise_evitada_rs REAL DEFAULT 0`
3. Recalcula os eventos existentes.

### 3.5 `app/templates/admin/novo_evento.html`

Adicionar uma linha na descrição dos campos explicando o efeito do
`foi_poluente` sobre α₁ (para o operador entender o que muda):

```
* foi poluente = sim: a1 = 30% da multa aplicada + custo de analise externa evitada
* foi poluente = nao: a1 = fracao do valor historico (autuacao errônea evitada) + custo de analise externa evitada
```

### 3.6 `CLAUDE.md`

Atualizar a seção "1.2 Equação refinada" (ou equivalente) para refletir a
nova definição de α₁. Também a seção 7 (dívidas técnicas) para remover o
item sobre "regra de zeragem do α₁" e adicionar os quatro novos parâmetros
como placeholders a validar via Delphi.

---

## 4. Testes de regressão

Depois da edição, rodar:

```bash
docker compose down
docker compose up -d --build
sleep 5
curl -fsS http://localhost:8000/health

# limpar eventos antigos (se já houver resultados calculados com a lógica antiga)
python3 scripts/00_init_db.py
python3 scripts/01_carregar_municipios.py
python3 scripts/02_ingest_alpha1_ibama.py
python3 scripts/03_ingest_alpha2_pesca.py
python3 scripts/04_ingest_alpha3_turismo.py
python3 scripts/05_ingest_alpha4_saude.py
python3 scripts/06_ingest_alpha5_ecossistemas.py
python3 scripts/07_carregar_parametros.py

# caso poluente: α₁ deve ser > 0
curl -c /tmp/c -b /tmp/c -d "username=admin&password=icseiom" -s http://localhost:8000/login
curl -c /tmp/c -b /tmp/c -L \
  -d "data_evento=2026-04-05&lat=-22.95&lon=-42.02&raio_km=25&foi_poluente=sim&descricao=regression_poluente" \
  -s http://localhost:8000/admin/novo-evento
curl -s http://localhost:8000/api/eventos | python3 -m json.tool

# caso nao-poluente: α₁ deve ser > 0 tambem, mas menor
curl -c /tmp/c -b /tmp/c -L \
  -d "data_evento=2026-04-06&lat=-22.95&lon=-42.02&raio_km=25&foi_poluente=nao&descricao=regression_nao_poluente" \
  -s http://localhost:8000/admin/novo-evento
curl -s http://localhost:8000/api/eventos | python3 -m json.tool
```

**Invariantes esperadas:**

1. α₁ poluente > α₁ não-poluente (para a mesma área/raio).
2. Ambos os α₁ > 0 (o custo de análise externa evitada é comum aos dois).
3. ICSEIOM do caso poluente > ICSEIOM do caso não-poluente.
4. Nenhum dos dois deve ser zero.

---

## 5. Escopo explícito desta task

**Fazer:**

- Corrigir `app/calc.py` com as fórmulas dos ramos A e B.
- Atualizar `scripts/07_carregar_parametros.py` com os 4 novos parâmetros.
- Migração de schema para separar os dois subcomponentes de α₁.
- Atualizar templates HTML.
- Atualizar `CLAUDE.md`.
- Rodar os testes de regressão e confirmar as invariantes.
- Rebuild do container e verificar `/health` OK.

**Não fazer nesta task** (ficam para depois, em outra sessão):

- Editar `ICSEIOM_proposta.docx`.
- Editar os três `.pptx`.
- Conectar ingestão real do SICAFI.
- Validação Delphi dos parâmetros.

Esses itens estão no arquivo companheiro
`../pendencias_alpha1_texto_slides.md`, que é a lista de pendências para
quando o Leandro retomar a edição dos documentos.

---

## 6. Convenções do projeto (resumo)

- Python 3.12, tipagem opcional.
- pt-BR em toda UI, templates, logs.
- Sem travessões longos em texto gerado para usuário.
- Decimais com vírgula em exibição (`R$ 1.234,56`).
- Não usar emoji em código.
- Paleta: navy `#21295C`, deep `#065A82`, teal `#1C7293`, gold `#C8A13A`.
- Nunca zerar valores monetários sem log/comentário explicando por quê.
- Sempre preservar o banco via volume `./db` do Docker Compose.
