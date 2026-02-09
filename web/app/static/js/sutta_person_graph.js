(async function () {
  const dataUrl = window.SUTTA_GRAPH_URL;
  const metaEl = document.getElementById("meta");
  const container = document.getElementById("graph");

  const resp = await fetch(dataUrl, { cache: "no-cache" });
  if (!resp.ok) {
    metaEl.textContent = `Failed to load graph (${resp.status})`;
    return;
  }

  const payload = await resp.json();
  const meta = payload.meta || {};
  const nodes = payload.nodes || [];
  const edges = payload.edges || [];
  if (!nodes.length) {
    metaEl.textContent = "No nodes found.";
    return;
  }

  const maxWeight = Math.max(meta.max_weight || 1, 1);
  const maxPr = Math.max(meta.max_pagerank || 1, 1);

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  const visNodes = new vis.DataSet(
    nodes.map((n) => {
      if (n.kind === "sutta") {
        return {
          id: n.id,
          label: n.label,
          shape: "box",
          size: 42,
          color: { background: "#f4c97a", border: "#8a5d00" },
          borderWidth: 2.5,
          font: { size: 20, face: "Avenir Next, Helvetica Neue, Arial", color: "#27211a" },
          fixed: true,
          x: 0,
          y: 0,
        };
      }

      const pr = n.pagerank || 0;
      const wt = n.weight || 0;
      // Log scaling keeps extreme pagerank hubs readable.
      const size = clamp(10 + Math.log1p(pr) * 4.8, 10, 34);
      const t = clamp(wt / maxWeight, 0, 1);
      const light = 86 - t * 40;
      return {
        id: n.id,
        label: n.label,
        shape: "dot",
        size,
        color: { background: `hsl(211, 70%, ${light}%)`, border: "#1f4d88" },
        borderWidth: 1.5,
        font: { size: 15, face: "Avenir Next, Helvetica Neue, Arial", color: "#11263f" },
        title: `${n.label}<br>weight: ${wt}<br>pagerank: ${pr.toFixed(2)}<br>verses: ${n.verse_count || 0}`,
      };
    })
  );

  const visEdges = new vis.DataSet(
    edges.map((e) => {
      const w = e.weight || 1;
      return {
        id: e.id,
        from: e.source,
        to: e.target,
        width: 1 + (w / maxWeight) * 12,
        color: { color: "rgba(73, 88, 106, 0.5)" },
        value: w,
        title: `weight: ${w} | verses: ${e.verse_count || 0}`,
      };
    })
  );

  const network = new vis.Network(
    container,
    { nodes: visNodes, edges: visEdges },
    {
      autoResize: true,
      interaction: { hover: true, navigationButtons: true, tooltipDelay: 90 },
      physics: {
        enabled: true,
        stabilization: { iterations: 250 },
        solver: "forceAtlas2Based",
        forceAtlas2Based: {
          gravitationalConstant: -120,
          springLength: 140,
          springConstant: 0.05,
        },
      },
      nodes: { shape: "dot" },
      edges: { smooth: { type: "dynamic" } },
    }
  );

  network.once("stabilizationIterationsDone", () => {
    network.setOptions({ physics: false });
    try {
      network.moveNode(`sutta:${meta.sutta_ref}`, 0, 0);
    } catch (e) {
      // ignore
    }
  });

  metaEl.textContent = `${meta.sutta_ref}: ${meta.verse_count} verses, ${meta.person_count} persons, ${meta.edge_count} links.`;
})();
