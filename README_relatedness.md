**Where we are**

- ATI relatedness pipeline is in place end-to-end:
  - Extract links: `graph/scripts/extract_related_links.py`
  - Score links (cosine/jaccard/person overlap): `graph/scripts/compute_related_baseline.py`
  - Write Neo4j edges: `graph/scripts/write_ati_related_edges.py`
- ATI web view was refocused from dense graph-first to **study-pairs first**:
  - Ranked discourse pairs table + click-to-focus graph: `web/app/templates/sutta_related_ati.html`, `web/app/static/js/sutta_related_ati.js`
  - Payload now includes `ranked_pairs`: `web/app/app.py`
- Relationship map styling updated:
  - Muted gray edges
  - Relatedness encoded by dash “solidness”
- Home route (`/`) links were updated in `web/app/templates/base.html` with examples for the new views.
- One bug fixed in Top Connected Verses:
  - Removed strict `pagerank IS NOT NULL` filter so it doesn’t return empty when PageRank is missing.

**Plan moving forward**

1. Validate each linked endpoint from `/` and list failures with exact error/response.  
2. Patch broken routes in `web/app/app.py` and related templates/js, one endpoint at a time.  
3. Add lightweight defensive handling:
   - graceful fallbacks when graph metrics are absent
   - clearer user-facing error text per view.  
4. Re-test all links from `base.html` and lock in with a short smoke-test checklist/script.  
5. Optional next step: add “study path” generation from top ranked ATI pairs.