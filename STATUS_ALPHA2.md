# Status α₂ Pesca — 2026-04-12

## Logica implementada

### Pipeline completo α₂

```
α₂(municipio, ano) = pesca_extrativa(municipio, ano) + aquicultura(municipio, ano)
```

**Pesca extrativa** (scripts 31):
1. BEPA CSV fornece toneladas por (UF, ano, familia taxonomica, setor)
2. Cada linha e classificada em 6 categorias de preco: peixes, camaroes, lagostas, siris/caranguejos, moluscos, outros
3. Distribuicao por municipio diferenciada por setor:
   - Artesanal → peso proporcional ao n de **pescadores** (SisRGP Pescador)
   - Industrial → peso proporcional ao n de **armadores** (SisRGP Armador)
4. Toneladas por municipio × preco da categoria = valor em R$

**Aquicultura** (script 30):
1. SIDRA 3940 var 215 fornece valor da producao (Mil Reais) por municipio, total
2. SIDRA 3940 var 4146 fornece quantidade por produto individual:
   - Peixes (79366), Camarao (32887), Ostras/vieiras/mexilhoes (32889) → unidade kg
   - Alevinos (32886), Larvas (32888), Sementes (32890) → unidade milheiros (nao somavel com kg)
3. Script baixa var 4146 por produto em kg, soma por municipio×ano, converte para toneladas
4. 1.561 de 3.392 celulas tem toneladas; as demais sao municipios que so produzem em unidades nao-kg

**Combinacao** (script 31):
- Para cada (municipio, ano), soma valor e toneladas de ambas as fontes
- Campo `fonte` registra origem: "BEPA-Art+RGP", "BEPA-Ind+Armador", "SIDRA 3940"

### Classificacao de precos por familia taxonomica

| Categoria | R$/ton | Familias | Fonte de preco |
|-----------|--------|----------|----------------|
| Peixes | 8.000 | todas as familias de Peixes | MPA Aquicultura 2023 (preco implicito) |
| Camaroes | 25.000 | Penaeidae, Solenoceridae, Pandalidae, Sergestidae | MPA Aquicultura 2023 |
| Lagostas | 80.000 | Palinuridae, Scyllaridae, Nephropidae | CEAGESP atacado |
| Siris/caranguejos | 15.000 | Portunidae, Ocypodidae, Grapsidae etc. | CEAGESP atacado |
| Moluscos | 15.000 | todas as familias de Moluscos | MPA Aquicultura 2023 |
| Outros | 10.000 | tudo que nao e Peixes, Crustaceos ou Moluscos | estimativa conservadora |

Precos armazenados em tabela `parametros`, ajustaveis via UI ou SQL.

### Distribuicao por setor (artesanal vs industrial)

| Setor | Peso | Fonte | Registros | Municipios |
|-------|------|-------|-----------|------------|
| Artesanal | n pescadores | SisRGP Pescador (17 CSVs por UF) | 1.434.543 | 3.427 |
| Industrial | n armadores | SisRGP Armador (CSV unico) | 3.971 | 171 |

Volume BEPA total: Artesanal 14,6M ton, Industrial 17,2M ton (ratio 1:1,17).

Fix aplicado: coluna UF do CSV Armador era o estado de registro RGP, nao do municipio.
Corrigido para extrair UF do campo `cidade` ("Itajai,SC,Brasil" → UF=SC).

### Normalizacao temporal

Funcao `media_anual()` em `app/main.py` calcula `AVG(valor)` por municipio
em vez de pegar so o ano mais recente. Resolve distorcao entre municipios
com dados ate 2022 (so BEPA) vs ate 2024 (BEPA + SIDRA).

### Toneladas da aquicultura (SIDRA 3940)

Problema original: var 4146 com classificacao total (654[0]) retorna `".."` para
todos os municipios/anos porque as unidades variam por produto (kg vs milheiros).
O IBGE nao soma unidades incompativeis.

Solucao: baixar var 4146 por produto individual, somente os que usam kg
(peixes=79366, camarao=32887, ostras/mexilhoes=32889), somar por municipio×ano,
converter kg → toneladas.

Exemplo Florianopolis 2024: 2.652,8 ton, R$ 39,6M (preco implicito ~R$ 14.940/ton).

## Resultado atual no banco

- 440 municipios, 4.999 celulas (2013-2024)
- R$ 53,1 bilhoes total (R$ 33,8B extrativa + R$ 19,3B aquicultura)
- Fontes: "BEPA-Art+RGP", "BEPA-Ind+Armador", "SIDRA 3940"
- 6 precos em `parametros`

## O que falta

### α₂ (pendencias menores)
1. **Metodologia HTML**: atualizar `app/templates/metodologia.html` com a separacao artesanal/industrial e a correcao das toneladas da aquicultura
2. **1.831 celulas aquicultura sem toneladas**: municipios que so produzem alevinos/larvas/sementes (unidade milheiros). Opcao: deixar NULL e mostrar "n/d" na tabela, ou buscar dado complementar no MPA Dados Abertos
3. **Validacao cruzada de precos**: tabela de correspondencia preco interno vs FOB (ComexStat) por categoria, para a apresentacao

### α₃ turismo (placeholder → dados reais)
- Fonte: IBGE SIDRA 5938, VAB alojamento e alimentacao por municipio
- Script `scripts/04_ingest_alpha3_turismo.py` tem stub `ingest_real()`

### α₄ saude (placeholder → dados reais)
- Fonte: DATASUS SIH (internacoes) e SINAN (notificacoes) por CIDs T52, T65, L23, L24, J68
- Script `scripts/05_ingest_alpha4_saude.py` tem stub `ingest_real()`

### α₅ ecossistemas (placeholder → dados reais)
- Fonte: MapBiomas Costeiro (ha manguezal, recife, restinga) + coeficientes TEEB/BPBES
- Script `scripts/06_ingest_alpha5_ecossistemas.py` tem stub `ingest_real()`

### Infraestrutura / UI
- Tabela de detalhe no mapa: mostrar "n/d" para toneladas NULL em vez de vazio
- Endpoints `/api/mb/alphas` e `/api/mb/detalhe` ja sao publicos (fix aplicado)
- Banner dinamico por alpha ja funciona
- Tabela generica por alpha ja funciona (a1-a5 + soma)

## Arquivos de dados externos

| Arquivo | Local | Uso |
|---------|-------|-----|
| BEPA CSV | `/Users/leandro/Downloads/Banco_reconstrucao_marinha_2026_02_11_Versao3_3(Plan1).csv` | Pesca extrativa por UF/ano/familia/setor |
| SisRGP Pescador | `/tmp/Pescador/` (17 CSVs) | Peso distribuicao artesanal |
| SisRGP Armador | `/Users/leandro/Downloads/Armador(Sheet1).csv` | Peso distribuicao industrial |
| SisRGP Empresa | `/Users/leandro/Downloads/Empresa Pesqueira(Sheet1).csv` | Nao usado (perfil misto) |
| ComexStat | `/tmp/EXP_2023.csv` | Validacao cruzada precos FOB |

## Comandos para reproduzir

```bash
# 1. aquicultura pura (SIDRA 3940, baixa da API ~2min)
python scripts/30_ingest_alpha2_pesca.py

# 2. extrativa BEPA + combinar com aquicultura
python scripts/31_ingest_alpha2_bepa.py \
  --bepa "/Users/leandro/Downloads/Banco_reconstrucao_marinha_2026_02_11_Versao3_3(Plan1).csv" \
  --rgp /tmp/Pescador \
  --armador "/Users/leandro/Downloads/Armador(Sheet1).csv"

# 3. reiniciar container
docker compose restart
```
