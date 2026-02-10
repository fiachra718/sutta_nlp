(async function () {
  const dataUrl = window.ATI_RELATED_DATA_URL;
  const container = document.getElementById("graph");
  const metaEl = document.getElementById("meta");
  const pairsBody = document.getElementById("pairs-body");

  const response = await fetch(dataUrl, { cache: "no-cache" });
  if (!response.ok) {
    metaEl.textContent = `Failed to load ATI relatedness data (${response.status})`;
    return;
  }
  const payload = await response.json();
  const { meta, nodes, edges, ranked_pairs: rankedPairs } = payload;

  if (!nodes.length || !edges.length || !rankedPairs.length) {
    metaEl.textContent = "No ATI related-link graph data for current filter.";
    return;
  }

  const maxCosine = Math.max(meta.max_cosine || 1, 1e-9);
  const minCosineSeen = Math.max(meta.min_cosine_seen || 0, 0);

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  function dashPattern(relatedness) {
    // Lower relatedness -> shorter dashes + larger gaps (visually "more broken").
    if (relatedness >= 0.92) return false;
    const dash = Math.max(2, Math.round(2 + relatedness * 5));
    const gap = Math.max(6, Math.round(6 + (1 - relatedness) * 14));
    return [dash, gap];
  }

  const visNodes = new vis.DataSet(
    nodes.map((n) => ({
      id: n.id,
      label: n.label,
      shape: "box",
      margin: 8,
      size: 18,
      color: {
        background: "#d4e3f7",
        border: "#2f4f7c",
      },
      borderWidth: 1.3,
      font: {
        face: "Avenir Next, Helvetica Neue, Arial",
        size: 14,
        color: "#142842",
      },
      title: n.label,
    }))
  );

  const baseEdgeStyle = new Map();
  const visEdges = new vis.DataSet(
    edges.map((e) => {
      const cosine = e.cosine || 0;
      const relatedness = clamp((cosine - minCosineSeen) / (maxCosine - minCosineSeen || 1), 0, 1);
      const style = {
        width: 1.3 + (relatedness * 0.6),
        color: { color: "#c3c9cf" },
        dashes: dashPattern(relatedness),
      };
      baseEdgeStyle.set(e.id, style);
      return {
        id: e.id,
        from: e.source,
        to: e.target,
        ...style,
        value: cosine,
        title: `cosine: ${cosine.toFixed(3)} | confidence: ${(e.confidence || 0).toFixed(2)} | kind: ${e.source_kind || ""}`,
        smooth: { type: "dynamic", roundness: 0.18 },
      };
    })
  );

  const network = new vis.Network(
    container,
    { nodes: visNodes, edges: visEdges },
    {
      autoResize: true,
      interaction: {
        hover: true,
        tooltipDelay: 100,
        navigationButtons: true,
      },
      physics: {
        enabled: true,
        stabilization: { iterations: 350 },
        solver: "forceAtlas2Based",
        forceAtlas2Based: {
          gravitationalConstant: -90,
          springLength: 120,
          springConstant: 0.05,
        },
      },
      edges: {
        arrows: { to: false },
        selectionWidth: 1.2,
      },
      layout: {
        improvedLayout: true,
      },
    }
  );

  network.once("stabilizationIterationsDone", () => {
    network.setOptions({ physics: false });
  });

  function signalScore(pair) {
    const cosine = pair.cosine || 0;
    const confidence = pair.confidence || 0;
    return (0.75 * cosine) + (0.25 * confidence);
  }

  const pairRows = rankedPairs
    .map((pair, idx) => ({
      rank: idx + 1,
      pair,
      signal: signalScore(pair),
    }));

  let selectedRow = null;
  let selectedEdgeId = null;

  function clearSelection() {
    if (selectedRow) {
      selectedRow.classList.remove("selected");
      selectedRow = null;
    }
    if (selectedEdgeId) {
      const baseStyle = baseEdgeStyle.get(selectedEdgeId);
      if (baseStyle) {
        visEdges.update({ id: selectedEdgeId, ...baseStyle });
      }
      selectedEdgeId = null;
    }
  }

  function focusPair(rowEl, pair) {
    clearSelection();
    selectedRow = rowEl;
    selectedRow.classList.add("selected");
    selectedEdgeId = pair.id;
    const baseStyle = baseEdgeStyle.get(pair.id) || {};
    visEdges.update({
      id: pair.id,
      ...baseStyle,
      color: { color: "#8f98a1" },
      width: (baseStyle.width || 1.6) + 0.9,
    });
    network.selectEdges([pair.id]);
    network.focus(pair.from_node_id, { scale: 1.15, animation: { duration: 400 } });
  }

  for (const row of pairRows) {
    const tr = document.createElement("tr");
    const overlap = `${row.pair.person_overlap}/${row.pair.person_union}`;
    tr.innerHTML = `
      <td>${row.rank}</td>
      <td><strong>${row.pair.from_ref}</strong> ↔ <strong>${row.pair.to_ref}</strong></td>
      <td>${row.pair.cosine.toFixed(3)}</td>
      <td>${overlap}</td>
      <td>${row.signal.toFixed(3)}</td>
    `;
    tr.addEventListener("click", () => focusPair(tr, row.pair));
    pairsBody.appendChild(tr);
  }

  if (pairsBody.firstElementChild) {
    pairsBody.firstElementChild.click();
  }

  metaEl.textContent = `Pairs: ${meta.pair_count}, nodes: ${meta.node_count}, edges: ${meta.edge_count}, cosine range: ${minCosineSeen.toFixed(3)}-${maxCosine.toFixed(3)}.`;
})();
