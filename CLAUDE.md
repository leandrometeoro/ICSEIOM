# CLAUDE.md — ICSEIOM Web

Contexto operacional e técnico para agentes Claude (Code ou Cowork) que forem
continuar o desenvolvimento deste projeto. Leia este arquivo inteiro antes de
editar qualquer coisa.

---

## 1. O que é o projeto

Aplicação web georreferenciada que opera o **ICSEIOM** — *Índice de Custo
Socioambiental Evitado por Incidentes com Óleo no Mar*, indicador proposto por
Leandro para a LGAF (Laboratório de Geoquímica Ambiental Forense) do IEAPM
(Instituto de Estudos do Mar Almirante Paulo Moreira / Marinha do Brasil),
apresentado na **11ª Oficina Lucro Social da DGDNTM**.

O ICSEIOM quantifica em reais o valor social **evitado** pela atuação preventiva
e forense da LGAF quando um incidente com óleo não se materializa (ou é contido).
É calculado por evento e somado ao longo do ano.

### Equação

```
ICSEIOM_anual = Σ_eventos [ k · (α₁ + α₂ + α₃ + α₄ + α₅) + β − χ ]
```

| Símbolo | Significado | Fonte de dados primária |
|---------|-------------|--------------------------|
| α₁ | Multa ambiental evitada | IBAMA SICAFI / Dados Abertos |
| α₂ | Pesca (extrativa + aquicultura) | BEPA/RGP + IBGE SIDRA 3940 |
| α₃ | Turismo costeiro (salários CNAE 55+56+79+93 × δ_costeiro) | IBGE SIDRA 6450 + MTur + QL (Cap. 36 Economia Azul) |
| α₄ | Saúde (custos SUS por CIDs T52/T65/L23/L24/J68) | DATASUS SIH/SINAN |
| α₅ | Ecossistemas (manguezal, recife, restinga) | MapBiomas Costeiro + TEEB/BPBES |
| β | Receitas LGAF | Tesouro Gerencial (UG IEAPM) |
| χ | Custos LGAF | SIAFI / Portal da Transparência |
| k | **Share LGAF** no mérito coletivo (≈ 0,30, **placeholder**) | Validação Delphi (pendente) |

O `k` **não é um fator estatístico genérico** — ele representa explicitamente
a fração do lucro social evitado atribuível à LGAF. Os `(1−k)` restantes vão
para os demais atores da cadeia de resposta (IBAMA fiscalizando, ICMBio
protegendo UC, Marinha contendo, Defesa Civil evacuando, municípios alertando
população). A LGAF contribui com **identificação forense** do poluente, que
aciona os demais; sem essa identificação, nem multa, nem alerta, nem resposta
coordenada acontecem. O valor 0,30 é placeholder a ser calibrado empiricamente
no painel Delphi descrito em `ICSEIOM_proposta.docx`. Esse mesmo `k` aplica-se
uniformemente a **todas** as parcelas α₁..α₅ — é o share da LGAF em cada
camada do lucro evitado, não só em uma delas.

**Regras importantes do cálculo:**

- Se o evento **foi poluente** (derrame confirmado), `α₁ = 0` — a multa foi
  aplicada, não evitada.
- β e χ são rateados por evento dividindo por `rateio = 10` (premissa de 10
  eventos/ano). Esse rateio é placeholder — ver seção 7.
- Para um município a uma distância `d` do epicentro dentro do raio `R`, a
  fração linear é `frac = max(0, 1 − d/R)`. Cada α é ponderado por essa fração.
- A "safra" de dados usada é o `MAX(ano)` disponível em α₁ até a data do evento.

---

## 2. Universo geográfico

Existem **duas tabelas de municípios** no banco:

1. **`municipios_costeiros`** (61 municípios): subconjunto representativo com
   código IBGE, nome, UF, região, população 2022, centróide (lat/lon) e
   polígono simbólico ~20 km em WKT. Usada pelo cálculo de eventos
   (`calc.py`) e pelo mapa GeoJSON.

2. **`municipios_brasil`** (5.570 municípios, dos quais **443 com
   `is_costeiro=1`**): tabela completa dos municípios brasileiros. Usada
   pelos scripts de ingestão de α₂ e α₃ para buscar dados em todo o
   universo costeiro, sem se limitar aos 61 da semente.

- Scripts de ingestão (α₂ pesca, α₃ turismo) consultam
  `municipios_brasil WHERE is_costeiro=1` e desabilitam `PRAGMA foreign_keys`
  ao inserir, porque as tabelas alpha têm FK para `municipios_costeiros`
  (que só tem 61). Os dados dos outros ~380 municípios ficam no banco e
  aparecem nas APIs `/api/mb/alphas` e `/api/mb/detalhe`.
- Geometria está em `municipios_costeiros.geom_wkt` (formato texto WKT).
  **Não** usamos SpatiaLite, tudo é Python puro (Haversine em `app/calc.py`).
  Se no futuro for necessário calcular interseções reais de polígonos,
  adicionar SpatiaLite ao Dockerfile.

---

## 3. Stack e arquitetura

```
sistema_icseiom_web/
├── Dockerfile              # python:3.12-slim + curl + sqlite3
├── docker-compose.yml      # 1 serviço, porta 8000, volume ./db
├── .env.example            # copiar para .env antes do primeiro up
├── .dockerignore
├── requirements.txt        # FastAPI, uvicorn, jinja2, itsdangerous, passlib
├── README.md               # instruções de usuário
├── CLAUDE.md               # este arquivo
│
├── app/
│   ├── __init__.py
│   ├── main.py             # FastAPI app: rotas públicas, API, auth, admin
│   ├── config.py           # lê env vars (DB_PATH, SECRET_KEY, ADMIN_*)
│   ├── db.py               # sqlite3 + helpers query_all/query_one/conn_ctx
│   ├── auth.py             # sessão por cookie assinado (SessionMiddleware)
│   ├── calc.py             # calcular_icseiom() + registrar_evento()
│   ├── templates/
│   │   ├── base.html       # layout + topbar + nav + footer (Leaflet CDN)
│   │   ├── index.html      # mapa público com municípios + eventos
│   │   ├── historico.html  # tabela de todos os eventos
│   │   ├── evento.html     # detalhe do evento + mini-mapa
│   │   ├── login.html
│   │   └── admin/
│   │       ├── dashboard.html
│   │       ├── novo_evento.html  # form com click-to-pick no mapa
│   │       └── fontes.html       # status + upload manual de safra
│   └── static/
│       ├── css/style.css   # paleta Ocean Gradient (NAVY/DEEP/TEAL/GOLD)
│       └── js/map.js       # Leaflet: carrega /api/municipios.geojson + /api/eventos
│
├── scripts/                # idênticos aos do sistema_icseiom original
│   ├── 00_init_db.py              # cria schema
│   ├── 01_carregar_municipios.py  # semeia 61 municípios
│   ├── 02_ingest_alpha1_ibama.py  # tem ingest_demo() + stub ingest_real()
│   ├── 03_ingest_alpha2_pesca.py   # versão demo
│   ├── 04_ingest_alpha3_turismo.py # dados reais: SIDRA 6450 + MTur + QL
│   ├── 05_ingest_alpha4_saude.py
│   ├── 06_ingest_alpha5_ecossistemas.py
│   ├── 07_carregar_parametros.py   # k, TEEB, β, χ, metadados_atualizacao
│   ├── 08..25_*.py                 # pipeline α₁ (ML, features, previsões)
│   ├── 30_ingest_alpha2_pesca.py   # aquicultura real (SIDRA 3940)
│   ├── 31_ingest_alpha2_bepa.py    # pesca extrativa BEPA + RGP + combina
│   ├── calcular_icseiom.py         # versão CLI standalone (duplica calc.py)
│   └── registrar_evento.py         # versão CLI standalone
│
└── db/
    └── icseiom.db          # SQLite pré-carregado
```

### Rotas FastAPI (`app/main.py`)

| Método | Caminho | Auth | Função |
|--------|---------|------|--------|
| GET | `/` | pública | Mapa + KPIs (`index.html`) |
| GET | `/historico` | pública | Tabela de eventos (`historico.html`) |
| GET | `/evento/{id}` | pública | Detalhe de 1 evento (`evento.html`) |
| GET | `/api/municipios.geojson` | pública | GeoJSON + Σα por município |
| GET | `/api/eventos` | pública | Lista de eventos + ICSEIOM |
| GET | `/api/fontes` | pública | Metadados das fontes |
| GET | `/health` | pública | Healthcheck Docker (`{"status":"ok"}`) |
| GET | `/login` | pública | Formulário de login |
| POST | `/login` | pública | Autentica (cria sessão) |
| GET | `/logout` | pública | Limpa sessão |
| GET | `/admin` | **admin** | Dashboard + status fontes |
| GET | `/admin/novo-evento` | **admin** | Formulário de novo incidente |
| POST | `/admin/novo-evento` | **admin** | Calcula e persiste |
| GET | `/admin/fontes` | **admin** | Painel de fontes |
| POST | `/admin/fontes/upload` | **admin** | Registra manualmente nova safra |

### Helper de template

O projeto usa Starlette 1.x, que exige `TemplateResponse(request, name, context)`.
**Não chame `TEMPLATES.TemplateResponse("x.html", ctx)` (API antiga) — dá erro
`TypeError: unhashable type: 'dict'`.** Use sempre:

```python
return render(request, "x.html", chave=valor, outra=valor)
```

definido em `main.py`. Ele injeta automaticamente `APP_TITLE`, `APP_SHORT`, `ORG`
e `user` (do cookie de sessão) no contexto.

### Autenticação

- `SessionMiddleware` do Starlette, cookie assinado com `ICSEIOM_SECRET`.
- Credenciais via env vars `ICSEIOM_ADMIN_USER` (default `admin`) e
  `ICSEIOM_ADMIN_PASSWORD` (default `icseiom`).
- Rotas admin usam `Depends(require_admin)` — se não logado, lança 303 com
  `Location: /login`.
- **Não** há criação de usuários: é autenticação de operador único. Para
  multi-usuário no futuro, adicionar tabela `usuarios` + `passlib` (já está em
  `requirements.txt`).

---

## 4. Esquema do banco SQLite

Todas as tabelas estão em `db/icseiom.db`. Schema completo em
`scripts/00_init_db.py`. Resumo:

```
municipios_costeiros (code_muni PK, nome, uf, regiao, area_km2, pop_2022,
                      lon_centro, lat_centro, geom_wkt)

municipios_brasil    (code_muni PK, nome, uf, regiao, is_costeiro INT,
                      setor_pngc INT)    -- 5.570 municipios, 443 costeiros

alpha1_multa_ambiental (code_muni, ano, valor_rs, n_autos, fonte)   PK: (code_muni, ano)
alpha2_pesca           (code_muni, ano, valor_rs, toneladas, fonte)
alpha3_turismo         (code_muni, ano, vab_aloj_rs, fonte)
alpha4_saude           (code_muni, ano, custo_rs, n_internacoes, fonte)
alpha5_ecossistemas    (code_muni, ano, ha_manguezal, ha_recife, ha_restinga,
                        valor_teeb_rs, fonte)

beta_receitas_lgaf  (ano PK, valor_rs, fonte)
chi_custos_lgaf     (ano PK, valor_rs, fonte)

parametros          (chave PK, valor, unidade, descricao)
                    -- chaves: k, teeb_manguezal, teeb_recife, teeb_restinga, rateio...

eventos             (id_evento PK AUTOINCREMENT, data_evento, lon, lat, raio_km,
                     foi_poluente INT 0/1, descricao, registrado_em TIMESTAMP)

eventos_municipios  (id_evento, code_muni, fracao)  PK: (id_evento, code_muni)

resultados          (id_evento PK, alpha1_rs..alpha5_rs, beta_rs, chi_rs,
                     k_aplicado, icseiom_rs, calculado_em)

metadados_atualizacao (fonte PK, ultima_safra, atualizado_em, url, observacoes)
```

**Chaves estrangeiras estão ativas** (`PRAGMA foreign_keys = ON` em `db.py`).
Os scripts de ingestão α₂ e α₃ usam `PRAGMA foreign_keys = OFF` ao inserir,
porque inserem dados de 443 municípios (de `municipios_brasil`) enquanto as
tabelas alpha têm FK para `municipios_costeiros` (61).

---

## 5. Como rodar

### Docker Compose (produção / pronto-para-qualquer-máquina)

```bash
cp .env.example .env          # edite se quiser trocar SECRET/senha
docker compose up -d --build
open http://localhost:8000    # ou xdg-open no Linux
```

A imagem é cacheada; rebuild só na primeira vez ou quando mudar
`requirements.txt`/`Dockerfile`. O volume `./db` persiste o SQLite entre
restarts — **não apague esta pasta sem backup se já houver eventos
registrados**.

### Python local (dev)

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Use `--reload` para dev (recarrega ao salvar arquivos).

### Logs e operação

```bash
docker compose logs -f              # stream
docker compose ps                   # status
docker compose down                 # parar (mantém volume)
docker compose down -v              # parar e apagar volume (cuidado!)
docker compose exec icseiom bash    # shell no container
```

---

## 6. Fluxos de dados

### Popular/atualizar α, β, χ

Cada script em `scripts/02..06` tem duas funções:

```python
def ingest_demo():   # insere placeholders proporcionais à população
def ingest_real():   # dados reais (implementado para α₂ e α₃)
```

**Status atual dos dados:**

| Alpha | Tipo | Script(s) | Municípios | Resultado |
|-------|------|-----------|------------|-----------|
| α₁ | Demo + ML (contrafactual) | 02, 10..25 | 61 | R$ estimado por ML |
| α₂ | **Real** | 30 + 31 | 440 | R$ 53,1B (2013-2024) |
| α₃ | **Real** | 04 | 295 | R$ 7,9B (2021) |
| α₄ | **Real** | 05 | 443 | R$ 1,12B (2023) |
| α₅ | **Real (MapBiomas Col 9 + UNEP-WCMC Reefs) + dupla valoração** | 06 + 32 + 33 + 34 | 443 | R$ 55,59B global / R$ 4,89B brasil |

**α₂ pesca** (`scripts/30_ingest_alpha2_pesca.py` + `scripts/31_ingest_alpha2_bepa.py`):
- Aquicultura: SIDRA 3940 (var 215 valor, var 4146 toneladas por produto em kg)
- Pesca extrativa: BEPA × precos por categoria × distribuicao artesanal/industrial (SisRGP)
- Detalhes completos em `STATUS_ALPHA2.md`

**α₃ turismo** (`scripts/04_ingest_alpha3_turismo.py --ano 2021`):
- `α₃(muni) = V_tur(muni) × δ_costeiro(muni)`
- V_tur = SIDRA 6450 CNAE 55+56+79+93 (salarios, R$ mil)
- δ_costeiro primario: ranking Sol e Praia do MTur, `(7 - rank) / 6`
- δ_costeiro fallback: Espec. Turistica (MTur) × QL normalizado (Cap. 36, Economia Azul)
- δ default para costeiros sem dados MTur: 0,10
- Usa `municipios_brasil WHERE is_costeiro=1` (443 municípios)
- Referencia CNAE: Freitas, Farias & Carvalho (2022), Tab. 4, Cap. 37, Economia Azul (DGN/Marinha)

**α₄ saude** (`scripts/05_ingest_alpha4_saude.py --ano 2023`):
- `α₄(muni) = pop(muni) × Σ_grupo [ casos_100k × custo_grupo ] / 1e5`
- 3 grupos sindromicos: dermatite (L23+L24), respiratoria (J68), toxica (T52+T65)
- Casos/100k (conservador, literatura DWH/Prestige): 200 dermatite + 350 respiratoria + 75 toxica = 625 total
- Custos SUS (SIH 2023): R$ 300 dermatite + R$ 5.000 respiratoria + R$ 8.000 toxica
- Custo por 100k hab = R$ 2,41 milhoes
- **Regra em calc.py**: α₄ so e somado ao ICSEIOM quando `foi_poluente=True`
  (inverso do α₁ e complementar ao α₃). A LGAF ao identificar o poluente aciona
  o alerta, evitando contato; em alarme falso nao ha custo a evitar.
- Populacao: municipios_costeiros.pop_2022 (61) + mb_muni_socio_anual (443)
- Referencias: Sandifer et al. (2014, DWH), Zock et al. (2007, Prestige), Soares et al. (2020, BR 2019)

**α₅ ecossistemas** (`scripts/06_ingest_alpha5_ecossistemas.py` + `32_ingest_mapbiomas_alpha5.py` + `33_ingest_unep_reefs_alpha5.py` + `34_carregar_centroides_ibge.py`):
- `α₅(muni) = ha_mang × coef_mang + ha_recife × coef_recife + ha_restinga × coef_restinga`
- **Mangue + Restinga (ha) reais por município**: MapBiomas Brasil Coleção 9 (DOI `10.58053/MapBiomas/VEJDZC`, safra 2023), arquivo `data/mapbiomas/mapbiomas_col9_cobertura_municipio.xlsx` (68 MB, MD5 `e999cf1d7c74445a162f9c615128a119`, baixado via `https://data.mapbiomas.org/api/access/datafile/179`). Classes: `5` (Mangue) → `ha_manguezal`, `49`+`50` (Restinga) → `ha_restinga`. Script `32` popula os 443 costeiros (438 com dado).
- **Recife de coral (ha) reais**: UNEP-WCMC Global Distribution of Coral Reefs v4.1 (DOI `10.34892/t2wk-5t34`), baixado via ArcGIS REST `https://data-gis.unep-wcmc.org/server/rest/services/HabitatsAndBiotopes/Global_Distribution_of_Coral_Reefs/FeatureServer/1/query` para a bbox brasileira. 11 polígonos, 697,57 km² = 69.757 ha. Script `33_ingest_unep_reefs_alpha5.py` usa **atribuição curada por bbox de região** (tentativa via centroide de município falha porque centroides ficam no interior enquanto os recifes estão offshore, puxando munis errados como Itacaré/Murici/Lauro de Freitas). As 4 regiões e âncoras oficiais:
  - **Banco de Abrolhos** (BA) → Caravelas/BA (2906907): 571,51 km² = 57.151 ha
  - **Costa dos Corais** (APA PE+AL) → Maragogi/AL (2704500): 111,19 km² = 11.119 ha
  - **Trindade/Martim Vaz** (ilhas oceânicas) → Vitória/ES (3205309): 12,73 km² = 1.273 ha
  - **Parcel Manuel Luís** (APA, MA) → Cururupu/MA (2103703): 2,15 km² = 215 ha
- **Centroides IBGE** para todos os 5570 munis em `municipios_brasil.lat_centro/lon_centro`, populados por `34_carregar_centroides_ibge.py` a partir de `app/static/data/municipios_br.geojson` (média dos vértices do anel externo, ok para nearest-neighbor). Desbloqueia scripts espaciais futuros sem depender do seed dos 61.
- **Totais reais**: 1.037.364 ha mangue + 69.757 ha recife + 810.611 ha restinga entre os 443 costeiros. Soure/PA lidera mangue com 43.099 ha (Marajó); Caravelas/BA lidera recife com 57.151 ha.
- **Dupla valoração** — duas colunas persistidas por município:
  - `valor_teeb_global_rs`: Costanza et al. 2014 (USD 9.990 manguezal, 5.115 recife, 491 restinga) × 5,0 R$/USD
  - `valor_teeb_brasil_rs`: CCARBON/USP (USD 215 manguezal amazônico) × 5,0; recife e restinga mantêm Costanza por falta de valoração brasileira específica publicada
- **Coluna ativa** `valor_teeb_rs` espelha a base selecionada em `parametros.alpha5_base` (`'global'` default, ou `'brasil'`). Helper `app.calc.set_alpha5_base(base)` troca a base e recalcula todos os eventos persistidos; UI em `/admin/fontes` tem o toggle.
- **Totais com recife incluído**: base global R$ 55,59B, base brasil R$ 4,89B. Diferença entre bases continua ~11x, dominada pelo manguezal (46x menor na base brasil).
- **Pipeline completo de ingestão**: `python3 scripts/32_ingest_mapbiomas_alpha5.py && python3 scripts/34_carregar_centroides_ibge.py && python3 scripts/33_ingest_unep_reefs_alpha5.py`. O 34 só precisa rodar uma vez (ou quando `municipios_br.geojson` mudar).
- Referências: MapBiomas Brasil Col 9 (Souza et al. 2020, *Remote Sensing* 12:2735); UNEP-WCMC et al. (2021) Global distribution of coral reefs v4.1; Costanza et al. (2014) *Global Environmental Change* 26:152–158; CCARBON/USP Amazônia.

Após atualizar qualquer fonte, registrar a nova safra em
`metadados_atualizacao` via:

- UI: `/admin/fontes` → formulário "Registrar atualização manual"
- SQL direto: `INSERT OR REPLACE INTO metadados_atualizacao ...`

### Registrar um novo evento

Fluxo da UI (`/admin/novo-evento`):

1. Usuário clica no mapa para definir lat/lon (ou digita nos campos).
2. Ajusta o raio (km) — o círculo é redesenhado em tempo real.
3. Escolhe data, se foi poluente, descrição.
4. POST → `registrar_evento()` em `app/calc.py`:
   - Chama `calcular_icseiom()` (Haversine contra os 61 centróides de `municipios_costeiros`).
   - `INSERT INTO eventos` → `id_evento`.
   - `INSERT INTO eventos_municipios` (um por município no raio).
   - `INSERT INTO resultados` (valores finais).
5. Redireciona para `/evento/{id_evento}`.

### Consulta pública

- Mapa na home carrega `/api/municipios.geojson` (GeoJSON com Σα por município)
  e `/api/eventos` (círculos dos incidentes com popup).
- Histórico em `/historico` lista todos os eventos com ICSEIOM calculado.
- Cada evento tem detalhamento em `/evento/{id}` com decomposição α₁–α₅, β, χ,
  k aplicado e lista de municípios impactados com fração.

---

## 7. Dívidas técnicas e pontos de atenção

Coisas que estão no projeto como **placeholder** ou **simplificação**:

1. **`k = 0,30` é chute.** Precisa passar pela **validação Delphi** descrita em
   `ICSEIOM_proposta.docx` (painel de 3 internos IEAPM/CHM/DHN + 3 externos
   UFRJ/INPE/IBAMA/ANP, critério 75% ≥ 4 na escala Likert). Idem para
   `rateio = 10` (em `app/calc.py`), `coeficientes TEEB` (em
   `scripts/07_carregar_parametros.py`) e pesos relativos entre os α.

2. **Dados majoritariamente reais.** α₂ (pesca), α₃ (turismo), α₄ (saúde) e α₅ (ecossistemas) usam
   dados reais de fontes oficiais (SIDRA, BEPA, MTur, SIH-SUS, MapBiomas Col 9) combinados
   com literatura revisada por pares (DWH, Prestige para α₄; Costanza 2014 / CCARBON/USP
   para α₅). α₁ usa estimativa por ML (contrafactual sobre a única série pública, IBAMA SIFISC).
   β e χ continuam placeholder.
   Os campos `fonte` em cada tabela alpha identificam a origem.
   α₄ tem lógica especial: só é contabilizado no ICSEIOM quando o evento foi
   poluente (inverso de α₁, complementar a α₃).
   α₅ tem escolha de base via `parametros.alpha5_base` ('global'|'brasil'),
   alternável no `/admin/fontes`; o helper `set_alpha5_base` recalcula
   automaticamente todos os eventos persistidos. Áreas (ha) reais por
   município: mangue + restinga via MapBiomas Col 9, recife de coral via
   UNEP-WCMC v4.1 (atribuição curada por bbox de região em 4 âncoras:
   Caravelas/BA, Maragogi/AL, Vitória/ES, Cururupu/MA).

3. **Duas tabelas de municípios com cobertura diferente.**
   `municipios_costeiros` tem 61 municípios com geometria (centróide + WKT),
   usada pelo cálculo de eventos e pelo mapa. `municipios_brasil` tem 443
   costeiros (`is_costeiro=1`), usada pelos scripts de ingestão α₂ e α₃.
   Os ~380 municípios extras aparecem nas APIs `/api/mb/*` mas não no mapa
   principal nem no cálculo de eventos. Para expandir o mapa, adicionar
   geometria real (IBGE Malhas Territoriais) a `municipios_costeiros`.

4. **Autenticação mínima.** Um único operador (`admin`). Sem usuário/grupo,
   sem audit log de quem fez o quê. Para Marinha em produção, adicionar tabela
   `usuarios`, hash com passlib/bcrypt, e preferencialmente SSO/LDAP.

5. **Sem HTTPS.** O container expõe HTTP na 8000. Para produção, colocar atrás
   de um reverse proxy (Caddy, Traefik, nginx) com certificado.

6. **Haversine contra 61 municípios em cada cálculo.** `calc.py` usa
   `municipios_costeiros` (61) para determinar quais municípios estão no raio
   de um evento. Com 443 costeiros seria necessário adicionar geometria
   (centróide) a `municipios_brasil` ou expandir `municipios_costeiros`.
   Com 61 é instantâneo; com 443 ainda seria ok.

7. **Rateio fixo β/χ por 10 eventos/ano.** Simplificação que supõe distribuição
   uniforme. Uma abordagem melhor é ratear proporcionalmente ao custo real
   do atendimento de cada evento (horas-técnico × tipo de análise).

8. **dV/dt do balanço de volume não está no ICSEIOM** — é outra linha de
   pesquisa do Leandro (tese de oceanografia física), não se mistura aqui.

9. **Códigos IBGE errados na seed de `municipios_costeiros` (bug sério).**
   Dos 61 municípios da seed, **51 têm `code_muni` que aponta para outro
   município no IBGE** (nome e centróide corretos, mas o número do código
   pertence a outra cidade). Exemplos: Ipojuca (2607901) na verdade é
   Jaboatão; Cairu (2908408) é Conceição do Coité; Porto Seguro (2933307)
   é Vitória da Conquista; Ilhéus (2917509) nem existe no IBGE. Impacto:
   quando `calc.py` calcula um evento e itera sobre os 61, ele faz lookup
   de α₂/₃/₄/₅ pelo `code_muni` — e essas tabelas foram populadas pelos
   scripts 30..34 usando códigos IBGE corretos de `municipios_brasil`.
   Resultado: o evento junta área de Jaboatão com nome "Ipojuca", etc.
   Os scripts 32 (MapBiomas) e 33 (UNEP Reefs) já operam sobre
   `municipios_brasil.is_costeiro=1` e não sofrem desse bug. **Correção
   pendente**: consultar `municipios_brasil` por `nome+uf` e reescrever
   `municipios_costeiros.code_muni` com os códigos IBGE corretos; depois
   auditar os eventos persistidos (se houver) para recalcular. Adiado
   até revisão explícita porque afeta qualquer evento já registrado.

---

## 8. Convenções de código

- Python 3.12, tipagem opcional (`dict | None`, `list[dict]` etc.).
- Formatação brasileira de moeda: `"R$ {:,.2f}".format(x).replace(",","X")
  .replace(".",",").replace("X",".")` — já aplicada nos templates.
- Paleta visual (Ocean Gradient):
  - `--navy: #21295C`
  - `--deep: #065A82`
  - `--teal: #1C7293`
  - `--gold: #C8A13A`
- Idioma: **português brasileiro** em toda a UI, templates, logs e comentários.
  Mensagens de erro, labels, tooltips — tudo em pt-BR.
- Sem emoji em código; aceitável só em texto solto quando explicitamente pedido.

---

## 9. Como Claude deve continuar

Se você é um agente Claude retomando este projeto:

1. **Leia primeiro:** este `CLAUDE.md`, depois `README.md`, depois
   `app/main.py` e `app/calc.py`. Os templates Jinja2 são secundários.

2. **Antes de editar:** rode `docker compose up -d --build` e confirme que
   `/health` responde OK. Se quebrou algo, bisect nos últimos commits.

3. **Pergunte antes de:**
   - Trocar o schema do banco (quebra retrocompatibilidade).
   - Substituir SQLite por Postgres/MySQL.
   - Trocar FastAPI por outra stack.
   - Remover os scripts `ingest_demo()` (quebra a experiência "roda em qualquer
     máquina").

4. **Pode editar livremente:**
   - CSS/HTML/JS dos templates.
   - Adicionar novas rotas API (`/api/*`).
   - Adicionar novos campos em formulários desde que acompanhem migração de
     schema.
   - Melhorar `ingest_real()` dos scripts para pegar dados de verdade.

5. **Leandro prefere:**
   - Respostas curtas e objetivas, sem postambulo.
   - pt-BR em tudo que é UI.
   - Não usar travessões longos (`—`), preferir vírgulas/parênteses/dois-pontos.
   - Evitar autoqualificação ("inovador", "inédito", "pela primeira vez").
   - Decimais com vírgula: `0,3` e não `0.3`.

6. **Contexto maior:** este sistema é parte de uma entrega da 11ª Oficina
   Lucro Social. Há três presentações (`dinamica 2.1 *.pptx`,
   `dinamica 2.2 *.pptx`) e um Word (`ICSEIOM_proposta.docx`) na pasta-mãe
   `LUCRO_SOCIAL/` que documentam o indicador em si. O sistema web é a
   **operacionalização** desse indicador.

---

## 10. Teste rápido de regressão

```bash
docker compose up -d --build
sleep 5
curl -fsS http://localhost:8000/health && echo
curl -fsS http://localhost:8000/api/municipios.geojson | python3 -c "import sys,json; d=json.load(sys.stdin); print('features:', len(d['features']))"
# deve imprimir: features: 61

# login + criação de evento
curl -c /tmp/c.txt -b /tmp/c.txt -d "username=admin&password=icseiom" \
  -s -o /dev/null -w "login: %{http_code}\n" http://localhost:8000/login
curl -c /tmp/c.txt -b /tmp/c.txt -L \
  -d "data_evento=2026-04-05&lat=-22.95&lon=-42.02&raio_km=25&foi_poluente=nao&descricao=smoke+test" \
  -s -o /dev/null -w "evento: %{http_code}\n" http://localhost:8000/admin/novo-evento
curl -s http://localhost:8000/api/eventos | python3 -m json.tool
```

Resultado esperado: `login: 303`, `evento: 200`, JSON com 1 evento Cabo Frio
de ICSEIOM ≈ R$ 255.389.870,31 (se a seed estiver intacta).

---

*Última atualização: 2026-04-12. Mantenedor: Leandro, com colaboração de
agentes Claude via Cowork mode e Claude Code.*
