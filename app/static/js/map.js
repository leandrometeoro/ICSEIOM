// ICSEIOM — mapa principal: municipios em cinza por default, dourado quando
// afetados por um evento. Sem bolinhas de raio.
(function(){
  const map = L.map('map').setView([-15, -40], 4);
  L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png', {
    maxZoom: 18, attribution: '© OpenStreetMap · © Carto'
  }).addTo(map);

  function brl(v){ return 'R$ ' + v.toLocaleString('pt-BR', {maximumFractionDigits:0}); }

  let muniLayer = null;
  let eventosData = [];
  // code_muni → {impacto_rs somado, eventos: []}
  let afetadosMap = new Map();

  function corImpacto(v){
    if(!v || v <= 0) return '#d7d7d7';
    const x = Math.log10(1 + v);
    if(x > 9) return '#21295C';
    if(x > 8.5) return '#065A82';
    if(x > 8) return '#1C7293';
    if(x > 7.5) return '#4A9DB8';
    if(x > 7) return '#8FC1D4';
    return '#C8A13A';
  }

  function styleFeat(f){
    const code = f.properties.code_muni;
    const hit = afetadosMap.get(code);
    if(hit){
      return {
        color: '#8a6a15', weight: 1.5,
        fillColor: corImpacto(hit.impacto_rs), fillOpacity: 0.8,
      };
    }
    return {
      color: '#888', weight: 0.5,
      fillColor: '#eaeaea', fillOpacity: 0.55,
    };
  }

  function renderMalha(costeirosOnly){
    const url = costeirosOnly
      ? '/api/municipios_br.geojson?costeiros=1'
      : '/api/municipios_br.geojson';
    if(muniLayer){ map.removeLayer(muniLayer); muniLayer = null; }
    fetch(url).then(r=>r.json()).then(gj=>{
      muniLayer = L.geoJSON(gj, {
        style: styleFeat,
        onEachFeature: (f, layer) => {
          const p = f.properties;
          const hit = afetadosMap.get(p.code_muni);
          let evHtml = '';
          if(hit && hit.eventos.length){
            evHtml = '<hr style="margin:.4rem 0"><strong>Eventos:</strong><br>' +
              hit.eventos.map(x =>
                `#${x.id_evento} (${x.data_evento}) frac ${x.fracao.toFixed(2)} · <a href="/evento/${x.id_evento}">ver</a>`
              ).join('<br>') +
              `<br><strong>Impacto atribuído:</strong> ${brl(hit.impacto_rs)}`;
          }
          layer.bindPopup(`
            <strong>${p.nome}</strong> <span style="color:#666">${p.uf}</span><br>
            <small>${p.regiao} · ${p.is_costeiro ? 'costeiro' : 'interior'}</small>
            ${evHtml}
          `);
          layer.on({
            mouseover: e => e.target.setStyle({weight: 2.5, color: '#C8A13A'}),
            mouseout: e => muniLayer.resetStyle(e.target),
          });
        }
      }).addTo(map);
      if(costeirosOnly && muniLayer.getBounds().isValid()){
        map.fitBounds(muniLayer.getBounds().pad(0.1));
      }
    });
  }

  // Eventos primeiro → popula afetadosMap → depois malha
  fetch('/api/eventos').then(r=>r.json()).then(evs=>{
    eventosData = evs;
    afetadosMap = new Map();
    evs.forEach(e => {
      const valorEvento = e.icseiom_rs || 0;
      (e.municipios_afetados||[]).forEach(m => {
        const cur = afetadosMap.get(m.code_muni) || {impacto_rs: 0, eventos: []};
        cur.impacto_rs += valorEvento * m.fracao;
        cur.eventos.push({
          id_evento: e.id_evento, data_evento: e.data_evento, fracao: m.fracao,
        });
        afetadosMap.set(m.code_muni, cur);
      });
    });
    renderMalha(true);
  });

  // Toggle costeiros × todos
  const toggle = L.control({position: 'topright'});
  toggle.onAdd = () => {
    const div = L.DomUtil.create('div', 'toggle');
    div.style.cssText = 'background:#fff;padding:.4rem .6rem;border-radius:4px;box-shadow:0 1px 4px rgba(0,0,0,.2);font-size:.82rem;user-select:none';
    div.innerHTML = `
      <label style="display:block;cursor:pointer"><input type="radio" name="escopo" value="costeiros" checked> só costeiros</label>
      <label style="display:block;cursor:pointer"><input type="radio" name="escopo" value="todos"> todos os munis</label>
    `;
    L.DomEvent.disableClickPropagation(div);
    div.addEventListener('change', e => {
      if(e.target.name === 'escopo') renderMalha(e.target.value === 'costeiros');
    });
    return div;
  };
  toggle.addTo(map);

  // Legenda — impacto atribuído (R$)
  const legend = L.control({position: 'bottomright'});
  legend.onAdd = () => {
    const div = L.DomUtil.create('div', 'legend');
    div.style.cssText = 'background:#fff;padding:.5rem .7rem;border-radius:4px;box-shadow:0 1px 4px rgba(0,0,0,.2);font-size:.78rem;line-height:1.35';
    div.innerHTML = '<strong>Impacto atribuído (R$)</strong><br>' +
      [
        ['#21295C', '> 10⁹'],
        ['#065A82', '10⁸·⁵'],
        ['#1C7293', '10⁸'],
        ['#4A9DB8', '10⁷·⁵'],
        ['#8FC1D4', '10⁷'],
        ['#C8A13A', '< 10⁷'],
        ['#eaeaea', 'sem evento'],
      ].map(([c, lbl]) =>
        `<span style="display:inline-block;width:14px;height:10px;background:${c};border:1px solid #888;margin-right:4px"></span>${lbl}`
      ).join('<br>');
    return div;
  };
  legend.addTo(map);
})();
