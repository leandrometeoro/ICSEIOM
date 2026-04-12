# ICSEIOM Web

Interface web georreferenciada para o **Índice de Custo Socioambiental Evitado por
Incidentes com Óleo no Mar** — LGAF / IEAPM / Marinha do Brasil.

Aplicação FastAPI + Leaflet empacotada em Docker. Consulta pública aberta, área
administrativa protegida por login.

## Funcionalidades

- **Mapa interativo** com os municípios costeiros do Decreto 5.300/2004 pintados
  pela soma de α (multa ambiental + pesca + turismo + saúde + ecossistemas).
- **Histórico público** de eventos registrados e seu ICSEIOM calculado.
- **Detalhe do evento** com decomposição α₁-α₅, β, χ e lista de municípios impactados.
- **Área administrativa** (login) para:
  - Registrar novo incidente (clicar no mapa → lat/lon/raio → cálculo automático).
  - Consultar status das fontes de dados (α, β, χ) e registrar atualizações.
- **API JSON** pública:
  - `GET /api/municipios.geojson` — GeoJSON dos municípios + Σα.
  - `GET /api/eventos` — eventos + ICSEIOM.
  - `GET /api/fontes` — metadados das fontes.
  - `GET /health` — healthcheck.

## Arquitetura

```
webapp/
├── Dockerfile              # Python 3.12-slim + uvicorn
├── docker-compose.yml      # 1 serviço, porta 8000, volume ./db
├── requirements.txt
├── .env.example            # copiar para .env e ajustar
├── app/
│   ├── main.py             # rotas FastAPI
│   ├── config.py           # lê env vars
│   ├── db.py               # wrappers SQLite
│   ├── auth.py             # sessão por cookie assinado
│   ├── calc.py             # calcular_icseiom + registrar_evento
│   ├── templates/          # Jinja2 (base, index, historico, evento, login, admin/*)
│   └── static/             # CSS + JS (map.js)
├── scripts/                # ingestão em Python (00_init_db, 01_carregar_municipios, …)
└── db/
    └── icseiom.db          # SQLite pré-carregado (61 municípios seed)
```

## Executando

### Opção 1 — Docker Compose (recomendada)

```bash
cp .env.example .env
# edite .env com sua ICSEIOM_SECRET e ICSEIOM_ADMIN_PASSWORD
docker compose up -d
```

Acesse <http://localhost:8000>.

### Opção 2 — Python local

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## Credenciais padrão

Usuário: `admin` · Senha: `icseiom` (troque via env vars `ICSEIOM_ADMIN_USER`/
`ICSEIOM_ADMIN_PASSWORD`).

## Atualizando fontes

Os scripts `02_ingest_alpha1_ibama.py` a `06_ingest_alpha5_ecossistemas.py` contêm
uma função `ingest_demo()` (valores placeholder) e um stub `ingest_real()` pronto
para ser ligado às APIs reais (IBAMA SICAFI, IBGE SIDRA, DATASUS, MapBiomas,
Tesouro Gerencial). Rode-os contra o `db/icseiom.db` persistido:

```bash
docker compose exec icseiom python scripts/02_ingest_alpha1_ibama.py
# ou, fora do container:
ICSEIOM_DB=./db/icseiom.db python3 scripts/02_ingest_alpha1_ibama.py
```

Em seguida, registre a nova safra em **Admin → Fontes**.

## Dados atuais (demo)

61 municípios costeiros (subconjunto do Decreto 5.300/2004) com α₁-α₅ populados
para 2024, k = 0,30, β e χ placeholder. Todos os valores estão marcados como demo
em `metadados_atualizacao.observacoes` e devem ser substituídos por ingestão real
antes de uso institucional.
