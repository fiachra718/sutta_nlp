(async function () {
  const dataUrl = window.GRAPH_DATA_URL;
  const container = document.getElementById("graph");
  const metaEl = document.getElementById("meta");

  const response = await fetch(dataUrl, { cache: "no-cache" });
  if (!response.ok) {
    metaEl.textContent = `Failed to load graph data (${response.status})`;
    return;
  }
  const payload = await response.json();
  const { meta, nodes, edges } = payload;

  if (!nodes.length) {
    metaEl.textContent = "No nodes found for this community.";
    return;
  }

  const maxStrength = Math.max(meta.max_strength || 1, 1);
  const maxWeight = Math.max(meta.max_weight || 1, 1);
  const centerId = meta.center_id;

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  function sizeByStrength(strength, isCenter) {
    // Compress very high-degree hubs so Buddha doesn't dominate the canvas.
    const base = 11 + Math.log1p(strength) * 5.2;
    const adjusted = isCenter ? base + 2 : base;
    return clamp(adjusted, 12, 46);
  }

  function nodeColor(strength, id) {
    if (id === centerId) {
      return "#f7b500";
    }
    const t = clamp(strength / maxStrength, 0, 1);
    const light = 85 - t * 45;
    return `hsl(205, 75%, ${light}%)`;
  }

  const visNodes = new vis.DataSet(
    nodes.map((n) => {
      const strength = n.strength || 0;
      const isCenter = n.id === centerId;
      const size = sizeByStrength(strength, isCenter);
      return {
        id: n.id,
        label: n.label,
        value: strength,
        title: `${n.label}<br>Strength: ${strength}<br>PageRank: ${(n.pagerank || 0).toFixed(2)}`,
        color: {
          background: nodeColor(strength, n.id),
          border: isCenter ? "#8a6300" : "#1f4f7a",
        },
        borderWidth: isCenter ? 2.2 : 1.5,
        size,
        font: {
          size: isCenter ? 18 : 15,
          face: "Avenir Next, Helvetica Neue, Arial",
          color: "#13293d",
        },
      };
    })
  );

  const visEdges = new vis.DataSet(
    edges.map((e) => {
      const w = e.weight || 1;
      const width = 1 + (w / maxWeight) * 10;
      return {
        from: e.source,
        to: e.target,
        value: w,
        width,
        title: `weight: ${w}`,
        color: { color: "rgba(84, 111, 135, 0.45)" },
        smooth: { type: "dynamic", roundness: 0.16 },
      };
    })
  );

  const network = new vis.Network(
    container,
    { nodes: visNodes, edges: visEdges },
    {
      autoResize: true,
      nodes: { shape: "dot" },
      edges: {
        scaling: { min: 1, max: 12 },
        selectionWidth: 1.4,
      },
      interaction: {
        hover: true,
        tooltipDelay: 100,
        navigationButtons: true,
      },
      physics: {
        enabled: true,
        stabilization: { iterations: 300 },
        forceAtlas2Based: {
          gravitationalConstant: -80,
          springLength: 120,
          springConstant: 0.06,
        },
        solver: "forceAtlas2Based",
      },
    }
  );

  network.once("stabilizationIterationsDone", () => {
    network.setOptions({ physics: false });
  });

  // Pin Buddha node to center after first render.
  network.on("afterDrawing", () => {
    try {
      network.moveNode(centerId, 0, 0);
    } catch (e) {
      // ignore
    }
  });

  const fallbackText = meta.fallback_used
    ? ` Requested community ${meta.requested_community} was empty; showing Buddha's community ${meta.effective_community}.`
    : "";
  metaEl.textContent = `Community ${meta.effective_community}: ${nodes.length} nodes, ${edges.length} edges.${fallbackText}`;
})();
