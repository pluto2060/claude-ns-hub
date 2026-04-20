// CTX Dashboard client — connects to /stream SSE and renders snapshots.
// All telemetry-sourced strings are escaped before insertion to prevent XSS
// (defense in depth — the telemetry source is trusted local hooks, but
// hooks may log user prompts in future extensions).
const $ = (id) => document.getElementById(id);

const ESC_MAP = {"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"};
function esc(s) {
  if (s == null) return "";
  return String(s).replace(/[&<>"']/g, c => ESC_MAP[c]);
}

let activityPlot = null;

function fmtThresholds(th) {
  return `Thresholds: CM ≥${th.cm_hybrid_min}%  |  g2_docs <${th.g2_docs_max}%  |  g2_grep <${th.g2_grep_max}%  |  p95 <${th.p95_max_ms}ms`;
}

function renderHealthRow(r) {
  const color = r.ok ? "green" : "yellow";
  const sym = r.ok ? "✓" : "~";
  const pct = Math.min(100, Math.max(2, Math.round((r.value || 0) * 100)));
  return `
    <div class="row">
      <div class="label">${esc(r.name)}</div>
      <div class="bar"><div class="bar-fill ${color}" style="width:${pct}%"></div></div>
      <div class="value">${esc(r.value_str)}</div>
      <div class="sym ${color}">${sym}</div>
      <div class="msg">${esc(r.msg)}</div>
    </div>`;
}

function renderLatencyBars(hist) {
  const total = hist.reduce((s, b) => s + b.count, 0) || 1;
  return hist.map(b => {
    const pct = (b.count / total) * 100;
    const danger = b.bucket === ">1s" || b.bucket === "500ms-1s";
    const fillColor = danger ? "red" : (b.bucket === "200-500ms" ? "yellow" : "green");
    return `
      <div class="b">
        <div class="bucket">${esc(b.bucket)}</div>
        <div class="bar"><div class="bar-fill ${fillColor}" style="width:${pct}%"></div></div>
        <div class="count">${esc(b.count)}</div>
      </div>`;
  }).join("");
}

function renderNotices(notices) {
  if (!notices || notices.length === 0) {
    return `<div style="color:var(--text-dim); font-family:var(--mono); font-size:12px;">— none —</div>`;
  }
  return notices.map(n => `
    <div class="notice">
      <div class="tilde">~</div>
      <div class="metric">${esc(n.metric)}</div>
      <div class="msg">${esc(n.msg)}</div>
    </div>`).join("");
}

function renderOther(other) {
  if (!other || other.length === 0) return `<span style="color:var(--text-dim); font-size:11px;">— none —</span>`;
  return other.map(o => `<span class="pill">${esc(o.label)} <span class="count">×${esc(o.count)}</span></span>`).join("");
}

function renderEvents(evs) {
  if (!evs || evs.length === 0) return `<div style="color:var(--text-dim); padding:10px;">No recent events.</div>`;
  return evs.map(e => `
    <div class="ev">
      <div class="t">${esc(e.time)}</div>
      <div class="ty">${esc(e.type)}</div>
      <div class="hk">${esc(e.hook || "")}</div>
      <div class="bk">${esc(e.block || "")}</div>
      <div class="dur">${e.duration_ms != null ? esc(e.duration_ms) + "ms" : ""}</div>
    </div>`).join("");
}

function renderActivity(activity) {
  const xs = activity.map(a => a.ts);
  const ys = activity.map(a => a.count);
  const opts = {
    width: $("activity-chart").clientWidth,
    height: 160,
    scales: { x: { time: true } },
    axes: [
      { stroke: "#7d8590", grid: { stroke: "rgba(255,255,255,0.04)" } },
      { stroke: "#7d8590", grid: { stroke: "rgba(255,255,255,0.04)" } },
    ],
    series: [
      {},
      {
        label: "events/min",
        stroke: "#58a6ff",
        fill: "rgba(88,166,255,0.15)",
        width: 2,
        points: { show: false },
      },
    ],
  };
  if (activityPlot) {
    activityPlot.setData([xs, ys]);
  } else {
    activityPlot = new uPlot(opts, [xs, ys], $("activity-chart"));
  }
}

function apply(snap) {
  if (snap.empty) {
    $("meta").textContent = snap.message || "no events";
    return;
  }
  $("meta").textContent = `${snap.total_events} events · window ${snap.window}`;
  $("updated").textContent = `Updated ${snap.updated}`;
  $("window").textContent = snap.window;
  const grade = $("grade");
  grade.textContent = snap.grade.label;
  grade.className = "grade " + snap.grade.style;
  $("health-rows").innerHTML = snap.rows.map(renderHealthRow).join("");
  renderActivity(snap.activity);
  $("latency-bars").innerHTML = renderLatencyBars(snap.latency_hist);
  $("notices").innerHTML = renderNotices(snap.notices);
  $("other").innerHTML = renderOther(snap.other);
  $("events").innerHTML = renderEvents(snap.recent);
  $("thresholds").textContent = fmtThresholds(snap.thresholds);
}

function connect() {
  const status = $("conn-status");
  const es = new EventSource("/stream");
  es.onmessage = (e) => {
    status.textContent = "live"; status.className = "conn ok";
    try { apply(JSON.parse(e.data)); } catch (err) { console.error(err); }
  };
  es.onerror = () => {
    status.textContent = "reconnecting…"; status.className = "conn err";
  };
}

// Initial fetch so the page is never blank while waiting for SSE
fetch("/api/snapshot").then(r => r.json()).then(apply).catch(console.error);
connect();

// Re-render chart on resize
window.addEventListener("resize", () => {
  if (activityPlot) {
    activityPlot.setSize({ width: $("activity-chart").clientWidth, height: 160 });
  }
});
