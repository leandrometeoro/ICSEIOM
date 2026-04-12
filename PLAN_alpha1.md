# Plano vivo — α₁ (multa ambiental evitada)

Mantido pelo agente Claude. Atualizado a cada etapa concluída.
Última atualização: 2026-04-11 (E6 concluída — modelo per-auto em produção).

## Status de execução

| Etapa | Scripts / artefatos | Status |
|---|---|---|
| E0 | requirements, Dockerfile libgomp1, migração metadados, /metodologia, topbar | ✓ concluído |
| E1.11 | IPCA BCB SGS 433 (555 meses, 1980-01 a 2026-03) | ✓ concluído |
| E1.12 | setor PNGC por UF (443 costeiros distribuídos) | ✓ concluído |
| E1.13 | pop SIDRA 6579 + PIB SIDRA 5938 | ✓ concluído |
| E1.14 | infra óleo (46 munis curados, 20 costeiros) | ✓ concluído |
| E2.15 | features por município (2132 células, 443 munis) | ✓ concluído |
| E2.16 | estratificação hierárquica PNGC × infra × PCA+GMM | ✓ concluído |
| E3.17 | Ridge + LightGBM, random + temporal | ✓ concluído |
| E4 | preencher /metodologia | ✓ gráficos + tabela de modelos |
| E5 | refatorar calc.py com ramos A/B | pendente |
| E6 | **re-ingestão per-auto IBAMA** | ✓ concluído (9.367 autos) |

### Números registrados

- IPCA: 555 meses (1980-01 a 2026-03)
- Distribuição PNGC costeiros: Norte 98, Nordeste 191, Leste 18, Sudeste 49, Sul 87 (= 443)
- Infra óleo: 20 costeiros com algum flag (ref=5, term=16, duto=7, eep=9)
- α₁ IBAMA: R$ 2.747.300.287,59 em 370 munis, 2005-2024
- mb_alpha1_autos: 9.367 autos individuais (após re-ingestão per-auto)
- mb_alpha1_auto_feat: 9.367 linhas, 35 features, 13 tipos, 5+ normas distintas
- Split aleatório 80/20 (métrica primária):
  - Ridge     R² = 0,142  MAE_log = 1,36  Spearman ρ = 0,40
  - LightGBM  R² = 0,360  MAE_log = 1,13  Spearman ρ = 0,62
- Split temporal 2005-2022 → 2023-2026 (diagnóstico de drift): R² negativo por
  regime change no mix de `tipo_infracao` (Qualidade Ambiental 5,8% → 46%;
  Controle Ambiental 56% → 16%).

### Resolução (2026-04-11)

Agregação por (muni, ano) tinha teto estatístico porque os discriminadores
reais (`TIPO_INFRACAO`, `NU_NORMA`, `ARTIGO`, `GRAVIDADE`) moram no nível do
auto. Re-ingestão per-auto destravou o modelo: LightGBM captura 36% da
variância em log-escala no split aleatório, desvio mediano de fator ~3x.
Modelo de produção: `alpha1_modelo_lgbm_random.txt`.

---

## Objetivo

Construir uma base estatística defensável para o valor esperado da multa
ambiental evitada (E[valor_multa]) usada nos ramos A e B da fórmula de α₁,
com validação por hold-out e comparação entre regressão linear (Ridge) e
gradient boosting (LightGBM).

## Princípios

- IPCA corrige valores monetários (multa, PIB) do ano do fato para abr/2026.
- População e PIB per capita são lidos no ano do fato (snapshot da época).
- Cluster data-driven por setor PNGC, k escolhido por silhouette (GMM com
  fallback KMeans).
- Fontes sempre anotadas em `metadados_atualizacao` e expostas em
  `/metodologia`.
- Nenhum parâmetro chutado sem marcação explícita de placeholder.

---

## Etapas

### Etapa 0 — Infraestrutura e página de metodologia
- [x] Adicionar `numpy`, `pandas`, `scikit-learn`, `lightgbm`, `matplotlib`
      ao `requirements.txt`.
- [ ] Rebuild do container com as novas deps.
- [ ] Estender `metadados_atualizacao` com colunas `nome_humano`, `orgao`,
      `url_portal`, `descricao_uso`, `script`, `observacoes_metodologicas`.
- [ ] Criar rota `/metodologia` (template `metodologia.html`) renderizando
      seção "Fontes de dados" dinâmica a partir da tabela estendida.
- [ ] Link "Metodologia" no topbar.
- [ ] Rodapé de camada no mapa-municipios com link pra `/metodologia#alpha1`.

### Etapa 1 — Dados auxiliares (séries temporais)
- [ ] **Script 11** `baixar_ipca.py` — BCB SGS 433, tabela `ipca_mensal`.
- [ ] **Script 12** `atribuir_setor_pngc.py` — mapeamento Decreto 5.300/2004,
      adiciona coluna `setor_pngc` em `municipios_brasil`.
- [ ] **Script 13** `baixar_pop_pib_historico.py` — SIDRA 6579 (pop anual) +
      SIDRA 5938 (PIB municipal anual), tabela `mb_muni_socio_anual`.
- [ ] **Script 14** `baixar_infra_oleo.py` — ANP dados abertos (terminais,
      dutos, refinarias, campos E&P) + ANTAQ (portos/TUPs), tabela
      `mb_infra_oleo`.

### Etapa 2 — Feature engineering e clustering
- [ ] **Script 15** `computar_features_muni.py` — join de tudo, aplica IPCA,
      calcula `multa_real_hoje`, `multa_frac_pib`, `multa_per_capita`,
      agrega por município, persiste em `mb_features_muni`.
- [ ] **Script 16** `estratificar_alpha1.py` — por setor PNGC: padroniza →
      PCA (≥85% var) → sweep k ∈ [2, min(8, n/10)] → GMM com fallback
      KMeans → escolhe k por silhouette, desempate por Davies-Bouldin.
      Persiste `mb_estratos_alpha1` + `diag_estratos.json`.

### Etapa 3 — Modelagem e validação
- [ ] **Script 17** `treinar_modelos_alpha1.py`:
    - target: `log(multa_real_hoje)` por auto
    - features: setor_pngc, cluster_id, ano, log(pop), log(pib_pc),
      tem_infra, norma, tipo_infracao, gravidade
    - split temporal (treino 2005-2022, teste 2023-2026) + split aleatório
      80/20 com 5-fold CV de sanity
    - modelos: Ridge + LightGBM
    - métricas: R², MAE (log), MdAPE (original), Spearman ρ, calibração
    - artefatos: `app/static/charts/alpha1_*.png` (6 gráficos),
      `mb_alpha1_modelos` (tabela), `alpha1_modelo_ridge.json`,
      `alpha1_modelo_lgbm.txt`

### Etapa 4 — Metodologia completa
- [ ] Preencher `/metodologia` com:
    - Equação do ICSEIOM e decomposição α₁
    - Tabela dinâmica de fontes (já funcional desde Etapa 0)
    - Pipeline de dados (Etapas 1-2) com diagramas
    - Resultados do clustering (PCA biplot, silhouette por setor)
    - Comparação de modelos (Ridge vs LightGBM, gráficos + tabela de
      métricas)
    - Feature importance e interpretação
    - Limitações e placeholders (f_apropriacao, custo_analise_externa,
      P_autuacao_erronea, recuperacao_defesa)
    - Seção "Como reproduzir" com comandos

### Etapa 5 — Integração com o cálculo
- [ ] Refatorar `app/calc.py` para:
    - Ler parâmetros dos ramos A/B da tabela `parametros`
    - Usar `E[valor_multa]` vindo do modelo treinado (predict via Ridge JSON
      ou LightGBM txt) em vez de soma municipal bruta
    - Decompor `alpha1_parcela_multa_rs` e `alpha1_analise_evitada_rs`
      em colunas separadas em `resultados`
- [ ] Atualizar `evento.html` com a decomposição
- [ ] Atualizar `admin/novo_evento.html` com explicação do efeito de
      `foi_poluente`

---

## Decisões consolidadas

| Decisão | Escolha | Justificativa |
|---|---|---|
| Agrupamento | PNGC (5 setores) + cluster intra-setor | Oficial + explica variância local |
| Seleção de k | Silhouette, tie-break Davies-Bouldin | Estável com n pequeno, interpretação direta |
| Algoritmo cluster | GMM → fallback KMeans | Permite clusters elipsoidais quando n permite |
| Target regressão | log(multa_real_hoje) | Cauda pesada, erros simétricos em log |
| Split primário | Temporal 2005-2022 → 2023-2026 | Mede capacidade preditiva real |
| Modelos | Ridge + LightGBM (ambos reportados) | Interpretabilidade vs poder preditivo |
| Correção monetária | IPCA (BCB SGS 433) até abr/2026 | Padrão nacional |
| Pop/PIB | Snapshot do ano do fato | Captura porte da época |

## Placeholders a validar depois (Delphi)

- `f_apropriacao` = 0,30 (fração da multa imputável ao LGAF)
- `p_autuacao_erronea` = 0,25
- `recuperacao_defesa` = 0,60
- `custo_analise_externa_rs` = 35.000 (por evento)
- `k` global = 0,30
- `rateio` β/χ = 10 eventos/ano

## Fora de escopo desta fase

- Ingestão real do SICAFI (fica para integração LGAF)
- Edição do `ICSEIOM_proposta.docx` e dos `.pptx`
- Validação Delphi dos parâmetros
- Dados de área afetada (descartado: SIFISC não tem, multa não é
  proporcional a área, fontes alternativas inviáveis no curto prazo)
