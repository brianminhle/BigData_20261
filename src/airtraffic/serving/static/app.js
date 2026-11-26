const $ = id => document.getElementById(id);
const svgNS = 'http://www.w3.org/2000/svg';
let current;
const formatTime = seconds => new Date(seconds * 1000).toLocaleString('vi-VN');
function element(tag, attrs = {}, text) {
  const node = document.createElementNS(svgNS, tag);
  Object.entries(attrs).forEach(([key, value]) => node.setAttribute(key, value));
  if (text !== undefined) node.textContent = text;
  return node;
}
function label(svg, x, y, text) { svg.append(element('text', {x, y}, text)); }
function metric(title, value, note) {
  const card = document.createElement('div'); card.className = 'metric';
  for (const [tag, text] of [['span', title], ['strong', value], ['small', note]]) {
    const node = document.createElement(tag); node.textContent = text; card.append(node);
  }
  return card;
}
function renderMap() {
  const svg = $('map'); svg.replaceChildren();
  const cells = current.cells.filter(row => row.window_start === Number($('window').value));
  if (!cells.length) { label(svg, 45, 160, 'Chưa có ô dữ liệu đã chốt trong khoảng chọn.'); return; }
  const west = Math.min(...cells.map(r => r.cell_lon_min));
  const east = Math.max(...cells.map(r => r.cell_lon_min)) + 1;
  const south = Math.min(...cells.map(r => r.cell_lat_min));
  const north = Math.max(...cells.map(r => r.cell_lat_min)) + 1;
  const step = Math.min(440 / (east - west), 310 / (north - south));
  const maximum = Math.max(...cells.map(r => r.distinct_aircraft));
  for (let lon = west; lon <= east; lon++) {
    const x = 60 + (lon - west) * step;
    svg.append(element('line', {x1:x, x2:x, y1:25, y2:25+(north-south)*step, class:'grid'}));
    label(svg, x, 50+(north-south)*step, `${lon}°E`);
  }
  for (let lat = south; lat <= north; lat++) {
    const y = 25 + (north - lat) * step;
    svg.append(element('line', {x1:60, x2:60+(east-west)*step, y1:y, y2:y, class:'grid'}));
    label(svg, 15, y+4, `${lat}°N`);
  }
  for (const cell of cells) {
    const x = 60 + (cell.cell_lon_min-west)*step, y = 25+(north-cell.cell_lat_min-1)*step;
    const rect = element('rect', {x:x+1, y:y+1, width:step-2, height:step-2,
      fill:`rgba(64,211,187,${0.25+0.75*cell.distinct_aircraft/maximum})`});
    const text = `${cell.cell_lat_min}°N, ${cell.cell_lon_min}°E: ${cell.distinct_aircraft} máy bay`;
    rect.append(element('title', {}, text));
    rect.addEventListener('mouseenter', () => $('map-note').textContent = text);
    svg.append(rect);
    if (step > 20) label(svg, x+step/2-4, y+step/2+4, cell.distinct_aircraft);
  }
  $('map-note').textContent = 'Màu sáng hơn: nhiều máy bay hơn. Ô trống: không có kết quả được xuất.';
}
function renderChart() {
  const svg = $('chart'); svg.replaceChildren();
  const rows = current.windows;
  if (!rows.length) { label(svg, 45, 160, 'Chưa có observations trong khoảng chọn.'); return; }
  const first = rows[0].window_start, end = rows.at(-1).window_start + 300;
  const peak = Math.max(1, ...rows.map(r => r.distinct_aircraft));
  for (let i = 0; i <= 4; i++) {
    const y = 330-i*70;
    svg.append(element('line', {x1:45, x2:535, y1:y, y2:y, class:'grid'}));
    label(svg, 10, y+4, Math.round(peak*i/4));
  }
  for (const row of rows) {
    const x = 45+(row.window_start-first)/(end-first)*490;
    const height = row.distinct_aircraft/peak*280;
    const rect = element('rect', {x, y:330-height, width:Math.max(1,300/(end-first)*490-2), height, fill:'#55cfc0'});
    rect.append(element('title', {}, `${formatTime(row.window_start)}: ${row.distinct_aircraft} ICAO duy nhất`));
    svg.append(rect);
  }
  label(svg, 45, 360, formatTime(first));
  label(svg, 330, 385, formatTime(end));
}
async function load() {
  $('refresh').disabled = true;
  try {
    const params = new URLSearchParams();
    if ($('scope').value) params.set('scope', $('scope').value);
    params.set('hours', $('range').value);
    const response = await fetch('/api/overview?'+params);
    if (!response.ok) throw new Error(`Không đọc được bản xuất (${response.status}).`);
    const previousGeneration = current?.generation;
    current = await response.json();
    if (previousGeneration && previousGeneration !== current.generation) clearMovement();
    $('scope').replaceChildren(...current.scopes.map((scope, index) => new Option(`Vùng quan sát ${index + 1}`,scope)));
    $('scope').value = current.scope;
    const summary = current.summary, count = summary.observations;
    const percent = number => count ? `${(100*number/count).toFixed(1)}%` : '—';
    $('metrics').replaceChildren(
      metric('Observations',count.toLocaleString('vi-VN'),'Trong khoảng thời gian chọn'),
      metric('Đỉnh máy bay / 5 phút',summary.peak_window_aircraft ?? '—','Distinct trên toàn vùng'),
      metric('Có tọa độ',percent(summary.with_position),'Tỷ lệ observations'),
      metric('Vị trí cũ > 15 giây',percent(summary.stale_positions),'Tỷ lệ observations'));
    const windows = [...new Set(current.cells.map(r => r.window_start))].sort((a,b)=>b-a);
    $('window').replaceChildren(...windows.map(t => new Option(formatTime(t),t)));
    $('freshness').textContent = current.checked_at ? `Serving kiểm tra: ${formatTime(current.checked_at)} · công bố: ${formatTime(current.published_at)}` : `Bản xuất: ${formatTime(current.generated_at)}`;
    $('status').className = '';
    const age = current.latest_snapshot ? Math.max(0,Math.floor(Date.now()/1000-current.latest_snapshot)) : null;
    $('status').textContent = age === null ? 'Chưa có dữ liệu nguồn.' : `Snapshot mới nhất: ${formatTime(current.latest_snapshot)} · cách đây ${Math.floor(age/60)} phút${age>300?' · Dữ liệu chưa cập nhật gần đây':''}`;
    renderMap(); renderChart();
  } catch (error) { $('status').className='error'; $('status').textContent=error.message; }
  finally { $('refresh').disabled=false; }
}
$('refresh').addEventListener('click',load);
function clearMovement() {
  $('movement-rows').replaceChildren();
  $('movement-status').textContent = 'Bộ lọc hoặc dữ liệu đã thay đổi; tra cứu lại máy bay để xem kết quả tương ứng.';
}
$('scope').addEventListener('change',()=>{clearMovement();load();});
$('range').addEventListener('change',()=>{clearMovement();load();});
$('window').addEventListener('change',renderMap);
load();

setInterval(() => { if (!document.hidden && !$('refresh').disabled) load(); }, 30000);
$('aircraft-form').addEventListener('submit', async event => {
  event.preventDefault();
  const params = new URLSearchParams({icao24:$('icao24').value, scope:$('scope').value, hours:$('range').value});
  try {
    const response = await fetch('/api/movement?'+params);
    if (!response.ok) throw new Error('Chưa có dịch vụ truy vấn chuyển động hoặc thông tin lọc chưa hợp lệ.');
    const result = await response.json();
    $('movement-rows').replaceChildren();
    for (const row of result.rows) {
      const tr = document.createElement('tr');
      for (const value of [formatTime(row.time_position), row.elapsed_seconds ?? '—',
        row.observed_speed_mps?.toFixed(1) ?? '—', row.altitude_change_m?.toFixed(1) ?? '—', row.observation_gap?'Có':'Không']) {
        const td = document.createElement('td'); td.textContent=value; tr.append(td);
      }
      $('movement-rows').append(tr);
    }
    $('movement-status').textContent = !result.analytics_run ? 'Chưa có batch analytics hoàn tất.' :
      `${result.rows.length} vị trí quan sát từ batch đã hoàn tất.${result.has_more?' Hiển thị trang đầu; API hỗ trợ offset/limit.':''} Dữ liệu batch có thể cũ hơn streaming.`;
  } catch (error) { $('movement-rows').replaceChildren(); $('movement-status').textContent=error.message; }
});
