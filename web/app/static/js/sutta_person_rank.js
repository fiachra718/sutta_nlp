(async function () {
  const dataUrl = window.RANK_DATA_URL;
  const metaEl = document.getElementById("meta");
  const bodyEl = document.getElementById("rank-body");

  const resp = await fetch(dataUrl, { cache: "no-cache" });
  if (!resp.ok) {
    metaEl.textContent = `Failed to load ranking (${resp.status})`;
    return;
  }
  const payload = await resp.json();
  const rows = payload.rows || [];
  metaEl.textContent = `Showing ${rows.length} suttas`;

  bodyEl.innerHTML = "";
  rows.forEach((r, i) => {
    const tr = document.createElement("tr");
    const suttaRef = r.sutta_ref || "";
    const href = `/suttas/${encodeURIComponent(suttaRef)}/persons`;
    tr.innerHTML = `
      <td>${i + 1}</td>
      <td><a href="${href}">${suttaRef}</a></td>
      <td>${Math.round(r.total_weighted_mentions || 0)}</td>
      <td>${Math.round(r.unique_persons || 0)}</td>
      <td>${(r.avg_weight_per_person || 0).toFixed(2)}</td>
    `;
    bodyEl.appendChild(tr);
  });
})();
