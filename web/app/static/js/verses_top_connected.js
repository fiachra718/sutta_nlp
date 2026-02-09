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
    metaEl.textContent = "No graph data returned.";
    return;
  }

  const maxVerseAvg = Math.max(meta.max_avg_person_pagerank || 1, 1);
  const maxPersonPr = Math.max(meta.max_person_pagerank || 1, 1);
  const maxRefCount = Math.max(meta.max_ref_count || 1, 1);

  function clamp(v, lo, hi) {
    return Math.max(lo, Math.min(hi, v));
  }

  const visNodes = new vis.DataSet(
    nodes.map((n) => {
      if (n.kind === "verse") {
        const t = clamp((n.avg_person_pagerank || 0) / maxVerseAvg, 0, 1);
        const size = 18 + Math.sqrt((n.avg_person_pagerank || 0) + 1) * 2.3;
        const light = 88 - t * 40;
        return {
          id: n.id,
          label: n.label,
          shape: "box",
          margin: 8,
          size,
          color: {
            background: `hsl(156, 55%, ${light}%)`,
            border: "#136f4a",
          },
          borderWidth: 1.6,
          font: {
            face: "Avenir Next, Helvetica Neue, Arial",
            color: "#0f172a",
            size: 14,
          },
          title: `${n.label}<br>person_degree: ${n.person_degree}<br>avg_person_pagerank: ${(n.avg_person_pagerank || 0).toFixed(2)}<br>${(n.text || "").slice(0, 220)}`,
        };
      }

      const t = clamp((n.person_pagerank || 0) / maxPersonPr, 0, 1);
      const size = clamp(9 + Math.log1p(n.person_pagerank || 0) * 3.8, 9, 30);
      const light = 88 - t * 45;
      return {
        id: n.id,
        label: n.label,
        shape: "dot",
        size,
        color: {
          background: `hsl(217, 75%, ${light}%)`,
          border: "#1d4f91",
        },
        borderWidth: 1.3,
        font: {
          face: "Avenir Next, Helvetica Neue, Arial",
          color: "#13293d",
          size: 14,
        },
        title: `${n.label}<br>pagerank: ${(n.person_pagerank || 0).toFixed(2)}`,
      };
    })
  );

  const visEdges = new vis.DataSet(
    edges.map((e) => {
      const w = e.ref_count || 1;
      return {
        id: e.id,
        from: e.source,
        to: e.target,
        value: w,
        width: 1 + (w / maxRefCount) * 8,
        color: { color: "rgba(72, 93, 118, 0.42)" },
        smooth: { type: "continuous" },
        title: `ref_count: ${w}`,
      };
    })
  );

  const network = new vis.Network(
    container,
    { nodes: visNodes, edges: visEdges },
    {
      autoResize: true,
      interaction: { hover: true, navigationButtons: true, tooltipDelay: 100 },
      physics: {
        enabled: true,
        stabilization: { iterations: 350 },
        forceAtlas2Based: {
          gravitationalConstant: -110,
          springLength: 100,
          springConstant: 0.05,
        },
        solver: "forceAtlas2Based",
      },
      layout: {
        improvedLayout: true,
      },
      edges: {
        selectionWidth: 1.3,
      },
    }
  );

  network.once("stabilizationIterationsDone", () => {
    network.setOptions({ physics: false });
  });

  metaEl.textContent = `Top ${meta.limit} verses: ${meta.verse_count} verse nodes, ${meta.person_count} person nodes, ${meta.edge_count} edges.`;
})();
