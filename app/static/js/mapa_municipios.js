// ICSEIOM — mapa de municipios (malha IBGE) com seletor de contribuicao α
(function(){
  const map = L.map('map').setView([-15, -52], 4);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
    maxZoom: 18, attribution: '© OpenStreetMap · © Carto'
  }).addTo(map);

  const BINS = [
    { color: '#c8ced6' },
    { color: '#bfd3df' },
    { color: '#7fa6bf' },
    { color: '#3f7a9f' },
    { color: '#1C7293' },
    { color: '#C8A13A' },
  ];
  const INTERIOR = { color: '#9aa3ad', fill: '#d7dbe0' };
  const SELECT   = { color: '#8a6f1f', fill: '#C8A13A' };

  const ALPHAS = {
    soma: { label: 'Σα',  titulo: 'Σα (todas as contribuições)' },
    a1:   { label: 'α̂₁', titulo: 'α̂₁ — valor esperado por auto IBAMA-óleo' },
    a2:   { label: 'α₂',  titulo: 'α₂ — pesca' },
    a3:   { label: 'α₃',  titulo: 'α₃ — turismo' },
    a4:   { label: 'α₄',  titulo: 'α₄ — saúde' },
    a5:   { label: 'α₅',  titulo: 'α₅ — ecossistemas' },
  };

  const state = {
    alpha: 'a1',
    onlyCoastal: true,
    regiao: '',
    uf: '',
    selectedCode: '',
    all: null,
    valores: {},       // {code_muni: {valor_rs, via, n_base}}
    thresholds: [0,0,0,0,0],
    layer: null,
  };

  function brl(v){
    return 'R$ ' + (v||0).toLocaleString('pt-BR', {maximumFractionDigits:0});
  }

  function binFor(valor){
    if (!valor || valor <= 0) return 0;
    for (let i = 0; i < state.thresholds.length; i++) {
      if (valor <= state.thresholds[i]) return i + 1;
    }
    return state.thresholds.length + 1;
  }

  function computeThresholds(values){
    const arr = values.filter(v => v > 0).sort((a,b) => a - b);
    if (!arr.length) return [0,0,0,0,0];
    const q = (p) => arr[Math.max(0, Math.min(arr.length-1, Math.floor(arr.length*p)))];
    return [q(0.20), q(0.40), q(0.60), q(0.80), arr[arr.length-1]];
  }

  function styleFor(feature){
    const p = feature.properties;
    if (state.selectedCode && p.code_muni === state.selectedCode) {
      return { color: SELECT.color, weight: 2, fillColor: SELECT.fill, fillOpacity: 0.9 };
    }
    if (!p.is_costeiro) {
      return { color: INTERIOR.color, weight: 0.3, fillColor: INTERIOR.fill, fillOpacity: 0.45 };
    }
    const entry = state.valores[p.code_muni];
    const v = entry ? (entry.valor_rs || 0) : 0;
    const b = binFor(v);
    return { color: '#065A82', weight: 0.5, fillColor: BINS[b].color, fillOpacity: 0.8 };
  }

  function filterFn(feature){
    const p = feature.properties;
    if (state.onlyCoastal && !p.is_costeiro) return false;
    if (state.regiao && p.regiao !== state.regiao) return false;
    if (state.uf && p.uf !== state.uf) return false;
    if (state.selectedCode && p.code_muni !== state.selectedCode) return false;
    return true;
  }

  const HERO = {
    a1: {
      title: 'Municípios do Brasil — α₁ multa ambiental (valor esperado por autuação)',
      desc: 'Malha IBGE 2022 (5.570 municípios) · 443 costeiros pintados pelo estimador α̂₁, valor esperado de multa IBAMA-óleo por auto, calculado como média geométrica do histórico do próprio município (via=muni) ou fallback hierárquico (setor PNGC + perfil de infra → setor PNGC). Universo de autos: filtro estrito Lei 9966/00 arts 15-17 ou Dec 4136/02 arts 29-45 (201 autos, SIFISC 2005-2026).',
    },
    a2: {
      title: 'Municípios do Brasil — α₂ produção pesqueira',
      desc: 'Valor da produção pesqueira e aquícola por município costeiro. Aquicultura: IBGE SIDRA 3940 (valor em R$, 2013-2024). Pesca extrativa marinha: BEPA/MPA (toneladas por UF, 1950-2022) distribuída por município via peso de pescadores SisRGP, convertida em R$ por preços diferenciados (peixes, camarões, lagostas, siris/caranguejos, moluscos). Fontes de preço: MPA Dados Aquicultura 2023, CEAGESP cotações atacado.',
    },
    a3: {
      title: 'Municípios do Brasil — α₃ turismo',
      desc: 'VAB de alojamento e alimentação por município costeiro. Dados placeholder (proporcionais à população municipal). Fonte definitiva: IBGE SIDRA 5938 + Cadastur/MTur.',
    },
    a4: {
      title: 'Municípios do Brasil — α₄ saúde',
      desc: 'Custos SUS por CIDs relacionados a exposição a hidrocarbonetos (T52, T65, L23, L24, J68). Dados placeholder. Fonte definitiva: DATASUS SIH/SINAN.',
    },
    a5: {
      title: 'Municípios do Brasil — α₅ ecossistemas',
      desc: 'Valor econômico de ecossistemas costeiros (manguezal, recife, restinga) via coeficientes TEEB/BPBES. Dados placeholder. Fonte definitiva: MapBiomas Costeiro.',
    },
    soma: {
      title: 'Municípios do Brasil — Σα (todas as contribuições)',
      desc: 'Soma de α₁ (multa ambiental) + α₂ (pesca) + α₃ (turismo) + α₄ (saúde) + α₅ (ecossistemas) por município costeiro. Valor mais recente disponível de cada componente.',
    },
  };

  const elHeroTitle = document.getElementById('hero-title');
  const elHeroDesc  = document.getElementById('hero-desc');

  function updateHero(){
    const h = HERO[state.alpha] || HERO.soma;
    elHeroTitle.textContent = h.title;
    elHeroDesc.textContent = h.desc;
  }

  const elCamada = document.getElementById('f-camada');
  const elCostas = document.getElementById('f-costeiros');
  const elReg    = document.getElementById('f-regiao');
  const elUf     = document.getElementById('f-uf');
  const elMun    = document.getElementById('f-mun');
  const elCount  = document.getElementById('f-count');
  const elTotal  = document.getElementById('f-total');
  const elReset  = document.getElementById('f-reset');
  const elScale  = document.getElementById('f-scale');
  const elAutosTitle = document.getElementById('autos-title');
  const elAutosSum   = document.getElementById('autos-sum');
  const elAutosTbody = document.getElementById('autos-tbody');

  function escapeHtml(s){
    return String(s == null ? '' : s).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    }[c]));
  }

  // Texto dos artigos que tipificam descarga de oleo — Lei 9966/00 e Dec 4136/02
  const ARTIGOS_TEXTO = {
    'Lei 9966/00': {
      '15': 'Art. 15. É proibida a descarga, em águas sob jurisdição nacional, de substâncias nocivas ou perigosas classificadas na categoria "A", definidas no art. 4º desta Lei, inclusive aquelas provisoriamente classificadas como tal, além de água de lastro, resíduos de lavagem de tanques ou outras misturas que contenham tais substâncias.',
      '16': 'Art. 16. É proibida a descarga, em águas sob jurisdição nacional, de substâncias classificadas nas categorias "B", "C" e "D", definidas no art. 4º desta Lei, inclusive aquelas provisoriamente classificadas como tal, quando o navio se encontrar dentro do porto organizado ou em área de fundeio.',
      '17': 'Art. 17. É proibida a descarga em águas sob jurisdição nacional de óleo, misturas oleosas e lixo, exceto nos casos permitidos pela Marpol 73/78, e o lançamento em águas sob jurisdição nacional de esgoto e águas servidas, a partir de plataformas e suas instalações de apoio, exceto nos casos permitidos por resolução do Conama.',
    },
    'Decreto 4136/2002': {
      '29': 'Art. 29. Incorre em infração quem descarregar em águas sob jurisdição nacional substância enquadrada na categoria "A", classificada de acordo com o art. 4º da Lei 9.966/00. Multa diária de R$ 7.000 a R$ 50.000.000.',
      '30': 'Art. 30. Incorre em infração quem descarregar em águas sob jurisdição nacional substância classificada na categoria "B". Multa de R$ 7.000 a R$ 50.000.000.',
      '31': 'Art. 31. Incorre em infração quem descarregar em águas sob jurisdição nacional substância classificada na categoria "C". Multa de R$ 7.000 a R$ 50.000.000.',
      '32': 'Art. 32. Incorre em infração quem descarregar em águas sob jurisdição nacional substância classificada na categoria "D". Multa de R$ 7.000 a R$ 50.000.000.',
      '33': 'Art. 33. Incorre em infração quem descarregar em águas sob jurisdição nacional substância classificada provisoriamente na categoria "A", "B", "C" ou "D". Multa de R$ 7.000 a R$ 50.000.000.',
      '36': 'Art. 36. Incorre em infração quem descarregar óleo, misturas oleosas ou lixo em águas sob jurisdição nacional, exceto nos casos permitidos pela Marpol 73/78. Multa de R$ 7.000 a R$ 50.000.000.',
      '37': 'Art. 37. Incorre em infração a plataforma ou suas instalações de apoio que lançarem em águas sob jurisdição nacional esgoto e águas servidas em desacordo com resolução Conama. Multa de R$ 7.000 a R$ 50.000.000.',
      '38': 'Art. 38. Incorre em infração a plataforma ou suas instalações de apoio que efetuarem o descarte contínuo de água de processo ou de produção em desacordo com a regulamentação ambiental específica (CONAMA 393/2007, limite TOG 29 mg/L mensal). Multa de R$ 7.000 a R$ 50.000.000.',
      '39': 'Art. 39. Incorre em infração quem não comunicar incidentes de descarga de substâncias nocivas de que trata o art. 22 da Lei 9.966/00. Multa de R$ 7.000 a R$ 1.000.000.',
      '42': 'Art. 42. Incorre em infração quem realizar pesquisa envolvendo descarga de substância nociva sem autorização do órgão ambiental competente. Multa de R$ 7.000 a R$ 50.000.000.',
      '43': 'Art. 43. Incorre em infração quem não comprovar que a descarga decorreu de situação de salvaguarda de vida humana no mar ou de segurança do navio. Multa de R$ 7.000 a R$ 50.000.000.',
      '44': 'Art. 44. Incorre em infração quem não comprovar que a descarga decorreu de avaria no navio ou em seus equipamentos, nos casos não previstos no art. 43. Multa de R$ 7.000 a R$ 50.000.000.',
      '45': 'Art. 45. Incorre em infração quem causar dano ambiental constatado por vistoria decorrente de descarga ou derrame de óleo ou substância nociva. Multa de R$ 7.000 a R$ 50.000.000.',
    },
  };

  function artigoTooltip(normaStr, artigoStr) {
    if (!normaStr || !artigoStr || artigoStr === '—') return '';
    const art = String(artigoStr).replace(/[°º]/g, '').trim();
    for (const [norma, arts] of Object.entries(ARTIGOS_TEXTO)) {
      if (normaStr.indexOf(norma.split(' ')[1]) >= 0 || normaStr.indexOf(norma) >= 0) {
        if (arts[art]) return arts[art];
      }
    }
    return '';
  }

  const elAutosHead  = document.getElementById('autos-thead');

  const DETALHE_HEADERS = {
    a1: ['Auto','Data','Valor (R$)','Tipo','Descrição','Gravidade','Norma','Artigo','Tipo auto','Efeito','Via'],
    a2: ['Ano','Valor (R$)','Toneladas','Fonte'],
    a3: ['Ano','VAB Aloj/Alim (R$)','Fonte'],
    a4: ['Ano','Custo SUS (R$)','Internações','Fonte'],
    a5: ['Ano','Valor TEEB (R$)','Manguezal (ha)','Recife (ha)','Restinga (ha)','Fonte'],
    soma: ['Componente','Valor (R$)'],
  };
  const DETALHE_TITULOS = {
    a1: 'Multa ambiental (valor esperado por autuação)',
    a2: 'Produção pesqueira',
    a3: 'Turismo',
    a4: 'Saúde',
    a5: 'Ecossistemas',
    soma: 'Resumo Σα',
  };

  function setTableHeaders(alpha){
    const cols = DETALHE_HEADERS[alpha] || DETALHE_HEADERS.a1;
    elAutosHead.innerHTML = '<tr>' + cols.map(c => {
      const cls = c.match(/R\$|Ton|ha|Inter/) ? ' class="num"' : '';
      return `<th${cls}>${c}</th>`;
    }).join('') + '</tr>';
  }

  function renderRowA1(a){
    const data = a.dt_fato || `${a.ano}-${String(a.mes||0).padStart(2,'0')}`;
    const norma = [a.tp_norma, a.nu_norma].filter(Boolean).join(' ');
    const tip = artigoTooltip(norma, a.artigo);
    const artTd = tip
      ? `<td class="art-has-tip" style="cursor:default; text-decoration:underline dotted; color:var(--deep);">${escapeHtml(a.artigo||'—')}<span class="art-balloon">${escapeHtml(tip)}</span></td>`
      : `<td>${escapeHtml(a.artigo||'—')}</td>`;
    return `<tr>
      <td class="mono">${escapeHtml(a.seq_auto)}</td>
      <td>${escapeHtml(data)}</td>
      <td class="num">${brl(a.valor_rs)}</td>
      <td>${escapeHtml(a.tipo_infracao||'—')}</td>
      <td class="wide">${escapeHtml(a.des_infracao||'—')}</td>
      <td>${escapeHtml(a.gravidade||'—')}</td>
      <td>${escapeHtml(norma||'—')}</td>
      ${artTd}
      <td>${escapeHtml(a.tipo_auto||'—')}</td>
      <td>${escapeHtml(a.efeito_meio_amb||'—')}</td>
      <td>${escapeHtml(a.match_via||'—')}</td>
    </tr>`;
  }

  function fmtTon(v){
    if (v == null) return '—';
    return v.toLocaleString('pt-BR', {maximumFractionDigits: 1});
  }
  function fmtHa(v){
    if (v == null) return '—';
    return v.toLocaleString('pt-BR', {maximumFractionDigits: 0});
  }

  function renderRowA2(r){
    return `<tr>
      <td>${r.ano}</td>
      <td class="num">${brl(r.valor_rs)}</td>
      <td class="num">${fmtTon(r.toneladas)}</td>
      <td class="small">${escapeHtml(r.fonte||'—')}</td>
    </tr>`;
  }
  function renderRowA3(r){
    return `<tr><td>${r.ano}</td><td class="num">${brl(r.valor_rs)}</td><td class="small">${escapeHtml(r.fonte||'—')}</td></tr>`;
  }
  function renderRowA4(r){
    return `<tr><td>${r.ano}</td><td class="num">${brl(r.valor_rs)}</td><td class="num">${(r.n_internacoes||0).toLocaleString('pt-BR')}</td><td class="small">${escapeHtml(r.fonte||'—')}</td></tr>`;
  }
  function renderRowA5(r){
    return `<tr><td>${r.ano}</td><td class="num">${brl(r.valor_rs)}</td><td class="num">${fmtHa(r.ha_manguezal)}</td><td class="num">${fmtHa(r.ha_recife)}</td><td class="num">${fmtHa(r.ha_restinga)}</td><td class="small">${escapeHtml(r.fonte||'—')}</td></tr>`;
  }
  function renderRowSoma(r){
    return `<tr><td>${escapeHtml(r.label)}</td><td class="num">${brl(r.valor_rs)}</td></tr>`;
  }

  const ROW_RENDERERS = { a1: renderRowA1, a2: renderRowA2, a3: renderRowA3, a4: renderRowA4, a5: renderRowA5, soma: renderRowSoma };

  function loadDetalhe(code, nome, uf){
    const alpha = state.alpha;
    const titulo = DETALHE_TITULOS[alpha] || alpha;
    elAutosTitle.textContent = `${titulo} — ${nome} (${uf})`;
    elAutosSum.textContent = 'carregando...';
    setTableHeaders(alpha);
    const ncols = (DETALHE_HEADERS[alpha] || []).length;
    elAutosTbody.innerHTML = `<tr><td colspan="${ncols}" class="autos-empty">carregando...</td></tr>`;

    fetch(`/api/mb/detalhe/${encodeURIComponent(code)}?alpha=${alpha}`)
      .then(r => r.json())
      .then(j => {
        if (!j.rows || !j.rows.length) {
          const msg = alpha === 'a1'
            ? 'Nenhum auto IBAMA sob filtro estrito de óleo (Lei 9966/00, Dec 4136/2002).'
            : `Nenhum dado de ${titulo.toLowerCase()} para este município.`;
          elAutosSum.textContent = `0 registros`;
          elAutosTbody.innerHTML = `<tr><td colspan="${ncols}" class="autos-empty">${msg}</td></tr>`;
          if (alpha === 'a1' && j.estimativa) {
            elAutosSum.textContent = `0 autos · estimativa α̂₁: ${brl(j.estimativa.alpha1_hat)} (via ${j.estimativa.via}, n=${j.estimativa.n_base})`;
          }
          return;
        }
        const renderRow = ROW_RENDERERS[alpha] || ROW_RENDERERS.a1;
        if (alpha === 'a1') {
          const extra = j.estimativa ? ` · α̂₁: ${brl(j.estimativa.alpha1_hat)} (${j.estimativa.via})` : '';
          elAutosSum.textContent = `${j.n_rows.toLocaleString('pt-BR')} autos · total ${brl(j.total_rs)}${extra}`;
        } else if (alpha === 'soma') {
          elAutosSum.textContent = `Σα = ${brl(j.total_rs)}`;
        } else {
          elAutosSum.textContent = `${j.n_rows.toLocaleString('pt-BR')} registros · total ${brl(j.total_rs)}`;
        }
        elAutosTbody.innerHTML = j.rows.map(renderRow).join('');
      })
      .catch(err => {
        elAutosSum.textContent = '';
        elAutosTbody.innerHTML = `<tr><td colspan="${ncols}" class="autos-empty">erro: ${escapeHtml(err.message)}</td></tr>`;
      });
  }

  function renderScale(){
    if (!state.thresholds.length) { elScale.innerHTML = ''; return; }
    const t = state.thresholds;
    const rows = [
      { sw: BINS[0].color, label: 'sem dado' },
      { sw: BINS[1].color, label: `até ${brl(t[0])}` },
      { sw: BINS[2].color, label: `até ${brl(t[1])}` },
      { sw: BINS[3].color, label: `até ${brl(t[2])}` },
      { sw: BINS[4].color, label: `até ${brl(t[3])}` },
      { sw: BINS[5].color, label: `até ${brl(t[4])}` },
    ];
    elScale.innerHTML = `<div class="scale-title">${ALPHAS[state.alpha].label} (R$)</div>` +
      rows.map(r => `<div class="scale-row"><span class="sw" style="background:${r.sw}"></span>${r.label}</div>`).join('');
  }

  function rebuild(){
    if (state.layer) map.removeLayer(state.layer);
    const filtered = {
      type: 'FeatureCollection',
      features: state.all.features.filter(filterFn),
    };
    state.layer = L.geoJSON(filtered, {
      style: styleFor,
      onEachFeature: (f, lyr) => {
        const p = f.properties;
        const entry = state.valores[p.code_muni];
        const v = entry ? entry.valor_rs : 0;
        const viaTxt = (state.alpha === 'a1' && entry && entry.via)
          ? ` <small>(via ${entry.via}, n=${entry.n_base})</small>` : '';
        const linha = p.is_costeiro
          ? (v ? `<br><strong>${ALPHAS[state.alpha].label}:</strong> ${brl(v)}${viaTxt}`
               : `<br><small>sem ${ALPHAS[state.alpha].label} registrado</small>`)
          : '<br><small>interior</small>';
        lyr.bindTooltip(`${p.nome} — ${p.uf}${linha}`, { sticky: true });
        lyr.on('click', () => {
          state.selectedCode = p.code_muni;
          elMun.value = p.code_muni;
          loadDetalhe(p.code_muni, p.nome, p.uf);
          rebuild();
          try { map.fitBounds(lyr.getBounds().pad(0.2)); } catch(e){}
        });
      },
    }).addTo(map);
    elCount.textContent = `${filtered.features.length.toLocaleString('pt-BR')} municípios exibidos`;
    if (filtered.features.length) {
      try { map.fitBounds(state.layer.getBounds().pad(0.05)); } catch(e){}
    }
  }

  function populateRegiaoOptions(){
    const regs = new Set();
    state.all.features.forEach(f => regs.add(f.properties.regiao));
    const arr = Array.from(regs).filter(Boolean).sort();
    elReg.innerHTML = '<option value="">Todas</option>' +
      arr.map(u => `<option value="${u}">${u}</option>`).join('');
  }
  function populateUfOptions(){
    const ufs = new Set();
    state.all.features.forEach(f => {
      if (state.regiao && f.properties.regiao !== state.regiao) return;
      ufs.add(f.properties.uf);
    });
    const arr = Array.from(ufs).sort();
    elUf.innerHTML = '<option value="">Todas</option>' +
      arr.map(u => `<option value="${u}">${u}</option>`).join('');
  }
  function populateMunOptions(){
    const codes = new Set();
    const opts = [];
    state.all.features.forEach(f => {
      const p = f.properties;
      if (state.onlyCoastal && !p.is_costeiro) return;
      if (state.regiao && p.regiao !== state.regiao) return;
      if (state.uf && p.uf !== state.uf) return;
      if (codes.has(p.code_muni)) return;
      codes.add(p.code_muni);
      opts.push({ code: p.code_muni, label: `${p.nome} — ${p.uf}` });
    });
    opts.sort((a,b) => a.label.localeCompare(b.label, 'pt-BR'));
    elMun.innerHTML = '<option value="">Todos</option>' +
      opts.map(o => `<option value="${o.code}">${o.label}</option>`).join('');
  }

  function loadAlpha(){
    elTotal.textContent = 'carregando...';
    return fetch(`/api/mb/alphas?alpha=${state.alpha}`)
      .then(r => r.json())
      .then(j => {
        state.valores = {};
        Object.entries(j.valores || {}).forEach(([c, d]) => {
          state.valores[c] = {
            valor_rs: d.valor_rs || 0,
            via: d.via || null,
            n_base: d.n_base || null,
          };
        });
        const arr = Object.values(state.valores).map(e => e.valor_rs);
        state.thresholds = computeThresholds(arr);
        const vals = arr.filter(v => v > 0);
        const rotulo = state.alpha === 'a1' ? 'munis estimados' : 'munis com dado';
        elTotal.innerHTML = `<strong>${ALPHAS[state.alpha].titulo}</strong><br>` +
          `${vals.length} ${rotulo} · média R$ ${(vals.reduce((a,b)=>a+b,0)/Math.max(vals.length,1)).toLocaleString('pt-BR',{maximumFractionDigits:0})}`;
        renderScale();
      })
      .catch(err => { elTotal.textContent = 'erro: ' + err.message; });
  }

  function applyCamada(){
    loadAlpha().then(rebuild);
  }

  elCamada.addEventListener('change', () => {
    state.alpha = elCamada.value;
    updateHero();
    applyCamada();
    if (state.selectedCode) {
      const feat = state.all.features.find(f => f.properties.code_muni === state.selectedCode);
      if (feat) loadDetalhe(state.selectedCode, feat.properties.nome, feat.properties.uf);
    }
  });
  elCostas.addEventListener('change', () => {
    state.onlyCoastal = elCostas.checked;
    state.selectedCode = '';
    populateMunOptions();
    rebuild();
  });
  elReg.addEventListener('change', () => {
    state.regiao = elReg.value;
    state.uf = '';
    state.selectedCode = '';
    populateUfOptions();
    populateMunOptions();
    rebuild();
  });
  elUf.addEventListener('change', () => {
    state.uf = elUf.value;
    state.selectedCode = '';
    populateMunOptions();
    rebuild();
  });
  elMun.addEventListener('change', () => {
    state.selectedCode = elMun.value;
    rebuild();
  });
  elReset.addEventListener('click', () => {
    state.alpha = 'a1';
    state.onlyCoastal = true;
    state.regiao = '';
    state.uf = '';
    state.selectedCode = '';
    elCamada.value = 'a1';
    elCostas.checked = true;
    elReg.value = '';
    updateHero();
    populateUfOptions();
    populateMunOptions();
    applyCamada();
    map.setView([-15, -52], 4);
  });

  elCount.textContent = 'carregando malha IBGE...';
  fetch('/api/municipios_br.geojson').then(r => {
    if (!r.ok) throw new Error('GeoJSON não encontrado.');
    return r.json();
  }).then(gj => {
    state.all = gj;
    try {
      populateRegiaoOptions();
      populateUfOptions();
      populateMunOptions();
    } catch(e){ console.error('populate erro:', e); }
    rebuild();
    loadAlpha().then(rebuild);
  }).catch(err => {
    elCount.textContent = err.message;
  });
})();
