import json
import logging
import os
from flask import Flask, render_template, abort, request, jsonify, url_for
from neo4j import GraphDatabase
from neo4j.exceptions import Neo4jError
from .models.models import CandidateDoc, TrainingDoc, SuttaVerse
from .api.ner import run_ner
from .render import render_highlighted
from pydantic import ValidationError
from .db import db
from .db.db import (
    list_nikayas,
    list_book_numbers,
    list_vaggas,
    get_ner_verse_spans,
    update_ner_verse_spans,
)

app = Flask(__name__)


def _configure_logger():
    logger = logging.getLogger("sutta_nlp.web")
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s %(name)s: %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    level_name = os.environ.get("APP_LOG_LEVEL", "DEBUG").upper()
    level = getattr(logging, level_name, logging.DEBUG)
    logger.setLevel(level)
    return logger


logger = _configure_logger()


def _parse_meta_value(value):
    if not value:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return None
    return None


def fetch_entity_id(entity_type: str, name: str, *, dsn: str | None = None) -> int | None:
    manager = SuttaVerse.objects.using(dsn) if dsn else SuttaVerse.objects
    return manager.fetch_entity_id(entity_type, name)


def _build_predict_payload(text: str, meta=None):
    doc = run_ner(text)
    payload = {
        "text": doc.get("text", text),
        "spans": doc.get("spans", []),
    }
    if meta:
        payload["meta"] = meta
    return payload


def _build_training_doc_context(doc: TrainingDoc):
    text = doc.text
    spans = []
    for span in doc.spans or []:
        span_data = span.model_dump() if hasattr(span, "model_dump") else {**span}
        start = span_data.get("start", 0)
        end = span_data.get("end", start)
        span_data["text"] = text[start:end]
        spans.append(span_data)
    html = render_highlighted(text, spans)
    raw_json = json.dumps(
        {"id": doc.id, "text": text, "spans": spans},
        ensure_ascii=False,
        indent=2,
    )
    return {
        "doc": doc,
        "text": text,
        "spans": spans,
        "html": html,
        "raw_json": raw_json,
    }

@app.route("/")
def home():
    return render_template("base.html", title="Jinja and Flask")


def _neo4j_settings():
    return {
        "uri": os.environ.get("NEO4J_URI", "bolt://localhost:7687"),
        "user": os.environ.get("NEO4J_USER", "neo4j"),
        "password": os.environ.get("NEO4J_PASSWORD", "testtest"),
        "database": os.environ.get("NEO4J_DATABASE", "neo4j"),
    }


def _fetch_center(tx, center_name: str):
    return tx.run(
        """
        MATCH (e:Entity {entity_type: 'PERSON'})
        WHERE toLower(e.canonical_name) = toLower($center_name)
        RETURN
          e.id AS id,
          e.canonical_name AS canonical_name,
          e.community_person_louvain AS community_id
        LIMIT 1
        """,
        center_name=center_name,
    ).single()


def _fetch_nodes(tx, community_id: int):
    return tx.run(
        """
        MATCH (e:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
        OPTIONAL MATCH (e)-[r:CO_MENTION_PERSON]-(:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
        WITH e, coalesce(sum(r.weight), 0) AS strength
        RETURN
          e.id AS id,
          e.canonical_name AS label,
          e.pagerank AS pagerank,
          strength
        ORDER BY strength DESC, label
        """,
        community_id=community_id,
    ).data()


def _fetch_edges(tx, community_id: int):
    return tx.run(
        """
        MATCH (a:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
              -[r:CO_MENTION_PERSON]-
              (b:Entity {entity_type: 'PERSON', community_person_louvain: $community_id})
        WHERE a.id < b.id
        RETURN
          a.id AS source,
          b.id AS target,
          r.weight AS weight
        ORDER BY weight DESC
        """,
        community_id=community_id,
    ).data()


def _load_community_payload(community_id: int, center_name: str):
    settings = _neo4j_settings()
    driver = GraphDatabase.driver(settings["uri"], auth=(settings["user"], settings["password"]))
    driver.verify_connectivity()

    with driver.session(database=settings["database"]) as session:
        center = session.execute_read(_fetch_center, center_name)
        if center is None:
            raise RuntimeError(f"Center entity '{center_name}' not found among PERSON entities.")

        requested_community = community_id
        effective_community = requested_community
        nodes = session.execute_read(_fetch_nodes, effective_community)

        if not nodes:
            effective_community = center["community_id"]
            nodes = session.execute_read(_fetch_nodes, effective_community)

        edges = session.execute_read(_fetch_edges, effective_community)

    driver.close()

    max_strength = max((n.get("strength") or 0 for n in nodes), default=0)
    max_weight = max((e.get("weight") or 0 for e in edges), default=0)

    return {
        "meta": {
            "requested_community": requested_community,
            "effective_community": effective_community,
            "center_label": center["canonical_name"],
            "center_id": center["id"],
            "fallback_used": requested_community != effective_community,
            "max_strength": max_strength,
            "max_weight": max_weight,
        },
        "nodes": nodes,
        "edges": edges,
    }


def _fetch_top_verses(tx, limit: int):
    return tx.run(
        """
        MATCH (v:Verse)-[:MENTIONS]->(e:Entity {entity_type: 'PERSON'})
        WHERE e.pagerank IS NOT NULL
        WITH v, count(DISTINCT e) AS person_degree, avg(e.pagerank) AS avg_person_pagerank
        ORDER BY person_degree DESC, avg_person_pagerank DESC
        LIMIT $limit
        RETURN
          v.id AS verse_id,
          v.sutta_ref AS sutta_ref,
          v.number AS verse_num,
          v.text AS verse_text,
          person_degree,
          avg_person_pagerank
        """,
        limit=limit,
    ).data()


def _fetch_mentions_for_verses(tx, verse_ids: list[int]):
    return tx.run(
        """
        UNWIND $verse_ids AS verse_id
        MATCH (v:Verse {id: verse_id})-[m:MENTIONS]->(p:Entity {entity_type: 'PERSON'})
        RETURN
          v.id AS verse_id,
          p.id AS person_id,
          p.canonical_name AS person_name,
          p.pagerank AS person_pagerank,
          coalesce(m.ref_count, 1) AS ref_count
        ORDER BY verse_id, person_name
        """,
        verse_ids=verse_ids,
    ).data()


def _load_top_connected_verses_payload(limit: int):
    settings = _neo4j_settings()
    driver = GraphDatabase.driver(settings["uri"], auth=(settings["user"], settings["password"]))
    driver.verify_connectivity()

    with driver.session(database=settings["database"]) as session:
        verses = session.execute_read(_fetch_top_verses, limit)
        verse_ids = [v["verse_id"] for v in verses if v.get("verse_id") is not None]
        mentions = session.execute_read(_fetch_mentions_for_verses, verse_ids) if verse_ids else []

    driver.close()

    nodes = []
    edges = []
    person_nodes: dict[int, dict] = {}

    max_avg = max((v.get("avg_person_pagerank") or 0 for v in verses), default=0)
    max_degree = max((v.get("person_degree") or 0 for v in verses), default=0)
    max_person_pr = max((m.get("person_pagerank") or 0 for m in mentions), default=0)
    max_ref_count = max((m.get("ref_count") or 0 for m in mentions), default=0)

    for v in verses:
        nodes.append(
            {
                "id": f"verse:{v['verse_id']}",
                "kind": "verse",
                "verse_id": v["verse_id"],
                "label": f"{v.get('sutta_ref') or 'Verse'}:{v.get('verse_num')}",
                "sutta_ref": v.get("sutta_ref"),
                "verse_num": v.get("verse_num"),
                "text": v.get("verse_text") or "",
                "person_degree": v.get("person_degree") or 0,
                "avg_person_pagerank": v.get("avg_person_pagerank") or 0.0,
            }
        )

    for m in mentions:
        person_id = m["person_id"]
        if person_id not in person_nodes:
            person_nodes[person_id] = {
                "id": f"person:{person_id}",
                "kind": "person",
                "person_id": person_id,
                "label": m.get("person_name") or f"Person {person_id}",
                "person_pagerank": m.get("person_pagerank") or 0.0,
            }

        edges.append(
            {
                "id": f"verse:{m['verse_id']}->person:{person_id}",
                "source": f"verse:{m['verse_id']}",
                "target": f"person:{person_id}",
                "ref_count": m.get("ref_count") or 1,
            }
        )

    nodes.extend(person_nodes.values())

    return {
        "meta": {
            "limit": limit,
            "verse_count": len(verses),
            "person_count": len(person_nodes),
            "edge_count": len(edges),
            "max_avg_person_pagerank": max_avg,
            "max_person_degree": max_degree,
            "max_person_pagerank": max_person_pr,
            "max_ref_count": max_ref_count,
        },
        "nodes": nodes,
        "edges": edges,
    }


def _fetch_sutta_person_rank_rows(tx, limit: int):
    return tx.run(
        """
        MATCH (s:Sutta)-[:HAS_VERSE]->(v:Verse)-[m:MENTIONS]->(p:Entity {entity_type: 'PERSON'})
        WITH s, p, sum(coalesce(m.ref_count, 1)) AS person_weight
        WITH
          s,
          sum(person_weight) AS total_weighted_mentions,
          count(p) AS unique_persons,
          avg(person_weight) AS avg_weight_per_person
        RETURN
          s.sutta_ref AS sutta_ref,
          total_weighted_mentions,
          unique_persons,
          avg_weight_per_person
        ORDER BY total_weighted_mentions DESC, unique_persons DESC
        LIMIT $limit
        """,
        limit=limit,
    ).data()


def _load_sutta_person_rank_payload(limit: int):
    settings = _neo4j_settings()
    driver = GraphDatabase.driver(settings["uri"], auth=(settings["user"], settings["password"]))
    driver.verify_connectivity()
    with driver.session(database=settings["database"]) as session:
        rows = session.execute_read(_fetch_sutta_person_rank_rows, limit)
    driver.close()
    return {"meta": {"limit": limit, "count": len(rows)}, "rows": rows}


def _fetch_sutta_person_graph_rows(tx, sutta_ref: str):
    return tx.run(
        """
        MATCH (s:Sutta {sutta_ref: $sutta_ref})
        MATCH (s)-[:HAS_VERSE]->(v:Verse)-[m:MENTIONS]->(p:Entity {entity_type: 'PERSON'})
        WITH
          s, p,
          count(DISTINCT v) AS verse_count,
          sum(coalesce(m.ref_count, 1)) AS weight,
          avg(p.pagerank) AS avg_person_pagerank
        RETURN
          s.sutta_ref AS sutta_ref,
          p.id AS person_id,
          p.canonical_name AS person_name,
          weight,
          verse_count,
          p.pagerank AS pagerank,
          avg_person_pagerank
        ORDER BY weight DESC, person_name
        """,
        sutta_ref=sutta_ref,
    ).data()


def _fetch_sutta_verse_count(tx, sutta_ref: str):
    return tx.run(
        """
        MATCH (s:Sutta {sutta_ref: $sutta_ref})-[:HAS_VERSE]->(v:Verse)
        RETURN count(DISTINCT v) AS verse_count
        """,
        sutta_ref=sutta_ref,
    ).single()


def _load_sutta_person_graph_payload(sutta_ref: str):
    settings = _neo4j_settings()
    driver = GraphDatabase.driver(settings["uri"], auth=(settings["user"], settings["password"]))
    driver.verify_connectivity()
    with driver.session(database=settings["database"]) as session:
        rows = session.execute_read(_fetch_sutta_person_graph_rows, sutta_ref)
        verse_count_row = session.execute_read(_fetch_sutta_verse_count, sutta_ref)
    driver.close()

    if not rows:
        raise RuntimeError(f"Sutta '{sutta_ref}' not found or has no PERSON mentions.")

    nodes = [
        {
            "id": f"sutta:{sutta_ref}",
            "kind": "sutta",
            "label": sutta_ref,
        }
    ]
    edges = []
    max_weight = max((r.get("weight") or 0 for r in rows), default=0)
    max_pagerank = max((r.get("pagerank") or 0 for r in rows), default=0)

    for r in rows:
        person_id = r["person_id"]
        nodes.append(
            {
                "id": f"person:{person_id}",
                "kind": "person",
                "person_id": person_id,
                "label": r["person_name"] or f"Person {person_id}",
                "pagerank": r.get("pagerank") or 0.0,
                "weight": r.get("weight") or 0,
                "verse_count": r.get("verse_count") or 0,
            }
        )
        edges.append(
            {
                "id": f"sutta:{sutta_ref}->person:{person_id}",
                "source": f"sutta:{sutta_ref}",
                "target": f"person:{person_id}",
                "weight": r.get("weight") or 0,
                "verse_count": r.get("verse_count") or 0,
            }
        )

    return {
        "meta": {
            "sutta_ref": sutta_ref,
            "verse_count": verse_count_row["verse_count"] if verse_count_row else 0,
            "person_count": len(rows),
            "edge_count": len(edges),
            "max_weight": max_weight,
            "max_pagerank": max_pagerank,
        },
        "nodes": nodes,
        "edges": edges,
    }


@app.route("/community/<int:community_id>")
def community_view(community_id: int):
    center = (request.args.get("center") or "Buddha").strip() or "Buddha"
    return render_template(
        "community_graph.html",
        title=f"Community {community_id} Graph",
        requested_community=community_id,
        center=center,
        data_url=url_for("community_data", community_id=community_id, center=center),
    )


@app.route("/api/community/<int:community_id>")
def community_data(community_id: int):
    center = (request.args.get("center") or "Buddha").strip() or "Buddha"
    try:
        payload = _load_community_payload(community_id, center)
    except RuntimeError as e:
        return jsonify({"ok": False, "message": str(e)}), 404
    except Neo4jError:
        logger.exception("Neo4j query failed for community=%s center=%s", community_id, center)
        return jsonify({"ok": False, "message": "Neo4j query failed."}), 500
    except Exception:
        logger.exception("Unexpected error loading community graph")
        return jsonify({"ok": False, "message": "Unexpected server error."}), 500

    return jsonify(payload)


@app.route("/suttas/person-rank")
def sutta_person_rank_view():
    limit = request.args.get("limit", 50, type=int) or 50
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500
    return render_template(
        "sutta_person_rank.html",
        title="Sutta Person Rank",
        limit=limit,
        data_url=url_for("sutta_person_rank_data", limit=limit),
    )


@app.route("/api/suttas/person-rank")
def sutta_person_rank_data():
    limit = request.args.get("limit", 50, type=int) or 50
    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500
    try:
        payload = _load_sutta_person_rank_payload(limit)
    except Neo4jError:
        logger.exception("Neo4j query failed for sutta person rank limit=%s", limit)
        return jsonify({"ok": False, "message": "Neo4j query failed."}), 500
    except Exception:
        logger.exception("Unexpected error loading sutta person rank")
        return jsonify({"ok": False, "message": "Unexpected server error."}), 500
    return jsonify(payload)


@app.route("/suttas/<path:sutta_ref>/persons")
def sutta_person_graph_view(sutta_ref: str):
    return render_template(
        "sutta_person_graph.html",
        title=f"{sutta_ref}: PERSON Mentions",
        sutta_ref=sutta_ref,
        data_url=url_for("sutta_person_graph_data", sutta_ref=sutta_ref),
    )


@app.route("/api/suttas/<path:sutta_ref>/persons")
def sutta_person_graph_data(sutta_ref: str):
    try:
        payload = _load_sutta_person_graph_payload(sutta_ref)
    except RuntimeError as e:
        return jsonify({"ok": False, "message": str(e)}), 404
    except Neo4jError:
        logger.exception("Neo4j query failed for sutta_ref=%s", sutta_ref)
        return jsonify({"ok": False, "message": "Neo4j query failed."}), 500
    except Exception:
        logger.exception("Unexpected error loading sutta person graph for %s", sutta_ref)
        return jsonify({"ok": False, "message": "Unexpected server error."}), 500
    return jsonify(payload)


@app.route("/verses/top-connected")
def top_connected_verses_view():
    limit = request.args.get("limit", 25, type=int) or 25
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200
    return render_template(
        "verses_top_connected.html",
        title="Top Connected Verses",
        limit=limit,
        data_url=url_for("top_connected_verses_data", limit=limit),
    )


@app.route("/api/verses/top-connected")
def top_connected_verses_data():
    limit = request.args.get("limit", 25, type=int) or 25
    if limit < 1:
        limit = 1
    if limit > 200:
        limit = 200
    try:
        payload = _load_top_connected_verses_payload(limit)
    except Neo4jError:
        logger.exception("Neo4j query failed for top connected verses limit=%s", limit)
        return jsonify({"ok": False, "message": "Neo4j query failed."}), 500
    except Exception:
        logger.exception("Unexpected error loading top connected verses graph")
        return jsonify({"ok": False, "message": "Unexpected server error."}), 500

    return jsonify(payload)


@app.route("/candidate/<int:candidate_id>")
def candidate(candidate_id: int):
    return render_template("doc.html", record=CandidateDoc.objects.get(candidate_id))

@app.route("/training/<string:training_id>")
def training(training_id: str):
    doc = TrainingDoc.objects.get(training_id)
    logger.debug("Fetched training doc %s: %s", training_id, bool(doc))
    if not doc:
        abort(404)
    context = _build_training_doc_context(doc)
    return render_template("view_doc.html", html=context["html"], raw_json=context["raw_json"])


@app.route("/training-doc/<string:doc_id>")
def training_doc(doc_id: str):
    doc = TrainingDoc.objects.get(doc_id)
    logger.debug("Training doc view request %s found=%s", doc_id, bool(doc))
    if not doc:
        abort(404)
    context = _build_training_doc_context(doc)
    return render_template("training_doc.html", **context)


'''
typical payload to the /speaker_span endpoint
{
  "meta": {
    "identifier": "mn10_tha",
    "verse_num": 12
  },
  "text": "Monks, this is the direct path…",
  "span": { "start": 123, "end": 210 },

  "speaker": { "type": "PERSON", "entity_id": 1, "text": "The Blessed One" },
  "interlocutor": { "type": "PERSON", "entity_id": 17, "text": "Ānanda" },
  "audience": { "type": "NORP", "entity_id": 991, "text": "bhikkhus" }
}
'''
@app.route("/speaker_span", methods=["GET", "POST"])
def speaker_span():
    if request.method == "GET":
        return render_template("speaker_span.html")
    data = request.get_json(force=True, silent=True) or {}
    meta = data.get("meta") if isinstance(data.get("meta"), dict) else {}
    identifier = (meta.get("identifier") or "").strip()
    verse_num = meta.get("verse_num")
    text = data.get("text") or ""
    span = data.get("span") or {}

    start = span.get("start")
    end = span.get("end")
    if not isinstance(start, int) or not isinstance(end, int) or end <= start:
        return jsonify({"ok": False, "message": "Span start/end must be integers with end > start."}), 400

    verse_row = None
    if identifier and verse_num is None:
        return jsonify({"ok": False, "message": "Verse number is required when identifier is provided."}), 400
    if identifier and verse_num is not None:
        try:
            verse_num = int(verse_num)
        except (TypeError, ValueError):
            return jsonify({"ok": False, "message": "Verse number must be an integer."}), 400
        verse_row = SuttaVerse.objects.fetch_verse_by_identifier(identifier, verse_num)
        if not verse_row:
            return jsonify({"ok": False, "message": "Verse not found for identifier/verse_num."}), 404
    else:
        if not isinstance(text, str) or not text.strip():
            return jsonify({"ok": False, "message": "Text is required to resolve verse."}), 400
        verse_row = SuttaVerse.objects.fetch_verse_by_cleaned_text(text)
        if not verse_row:
            return jsonify({"ok": False, "message": "Unable to resolve verse from cleaned text."}), 404
        identifier = verse_row.get("identifier")
        verse_num = verse_row.get("verse_num")

    meta = {
        "identifier": identifier,
        "verse_num": verse_num,
        "verse_id": verse_row.get("id") if verse_row else None,
    }

    def resolve_entity(role_name: str, required: bool):
        payload = data.get(role_name)
        if payload is None:
            if required:
                return None, f"{role_name} is required."
            return None, None
        if not isinstance(payload, dict):
            return None, f"{role_name} must be an object."
        role_type = (payload.get("type") or "").strip().upper()
        role_text = (payload.get("text") or "").strip()
        if not role_type or not role_text:
            return None, f"{role_name} requires both type and text."
        if role_type not in {"PERSON", "NORP"}:
            return None, f"{role_name} type must be PERSON or NORP."
        entity_id = fetch_entity_id(role_type, role_text)
        if not entity_id:
            return None, f"{role_name} not found in canonical entities."
        return {"type": role_type, "text": role_text, "entity_id": entity_id}, None

    speaker, error = resolve_entity("speaker", True)
    if error:
        return jsonify({"ok": False, "message": error}), 400
    interlocutor, error = resolve_entity("interlocutor", False)
    if error:
        return jsonify({"ok": False, "message": error}), 400
    audience, error = resolve_entity("audience", False)
    if error:
        return jsonify({"ok": False, "message": error}), 400

    resolved = {
        "meta": meta,
        "text": text,
        "span": {"start": start, "end": end},
        "speaker": speaker,
        "interlocutor": interlocutor,
        "audience": audience,
    }
    verse_id = meta.get("verse_id")
    if verse_id is None:
        return jsonify({"ok": False, "message": "Unable to resolve verse id."}), 400
    result = SuttaVerse.objects.update_discourse_spans(verse_id, resolved)
    if not result.get("ok"):
        return jsonify({"ok": False, "message": result.get("message", "Update failed.")}), 400
    return jsonify({"ok": True, "payload": resolved, "updated": result.get("updated", 0)})


@app.route("/predict", methods=["GET", "POST"])
def predict_page():
    if request.method == "GET":
        return render_template("predict.html", initial_doc=None)

    data = request.get_json(force=True, silent=True) or {}
    text = (data.get("text") or request.form.get("text") or "").strip()
    logger.debug("Predict request text length=%d meta=%s (json=%s form=%s)",
                 len(text),
                 bool(data.get("meta") or request.form.get("meta")),
                 request.is_json,
                 bool(request.form))
    if not text:
        return jsonify({"ok": False, "error": "text required"}), 400
    meta = _parse_meta_value(data.get("meta"))
    if not meta:
        meta = _parse_meta_value(request.form.get("meta"))

    payload = _build_predict_payload(text, meta)
    logger.debug("Predict payload spans=%d meta=%s", len(payload.get("spans", [])), bool(meta))

    if request.is_json:
        return jsonify({"ok": True, **payload})

    return render_template("predict.html", initial_doc=payload)


@app.post("/api/training")
def save_training_doc():
    data = request.get_json(force=True, silent=True) or {}
    text = data.get("text", "")
    spans = data.get("spans", [])
    logger.debug(data)

    if not isinstance(text, str) or not text.strip():
        return jsonify({"ok": False, "message": "Text is required."}), 400

    try:
        doc = TrainingDoc.model_validate({
            "id": data.get("id"),
            "text": text,
            "spans": spans,
            "source": "manual",
        })
    except ValidationError as exc:
        messages = "; ".join(error.get("msg", "invalid value") for error in exc.errors())
        return jsonify({"ok": False, "message": f"Invalid training doc: {messages}"}), 400

    result = TrainingDoc.objects.save(doc, source="manual")

    if result.get("ok"):
        return jsonify({"ok": True, "id": result["id"], "message": "Saved to training data."}), 201

    return jsonify({
        "ok": False,
        "id": result.get("id"),
        "message": result.get("message", "Unable to save training doc."),
    }), 409

@app.route("/random")
def random_verse():
    verse = SuttaVerse.random_with_titlecase()
    if not verse:
        logger.warning("random_verse: no verse matched titlecase criteria")
        abort(404)

    initial_doc = {
        "text": verse.text,
        "spans": [],
        "meta": {
            "identifier": verse.identifier,
            "verse_num": verse.verse_num,
        },
    }
    return render_template("random.html", initial_doc=initial_doc)

@app.route("/verses/facets", methods=["GET", "POST"])
def facets():
    if request.method == "GET":
        initial_doc = None
        return render_template("facet_form.html", initial_doc=initial_doc)
    data = request.get_json(force=True, silent=True) or {}
    label = (data.get("label") or "").strip().upper()
    terms = data.get("terms") or data.get("term") or []
    if not label:
        for key in ("person", "gpe", "loc"):
            value = data.get(key)
            if value:
                label = key.upper()
                terms = value
                break
    if isinstance(terms, str):
        terms = [term.strip() for term in terms.split(",")]

    allowed_labels = {"PERSON", "GPE", "LOC"}
    if label not in allowed_labels:
        return jsonify({"ok": False, "error": f"label must be one of {sorted(allowed_labels)}"}), 400
    if not terms:
        return jsonify({"ok": False, "error": "terms is required"}), 400

    rows = SuttaVerse.objects.facet_search(label=label, terms=terms)
    return jsonify({"ok": True, "count": len(rows), "items": rows})


@app.route("/verses/browse")
def browse_verses():
    nikaya = request.args.get("nikaya") or ""
    book_number = request.args.get("book_number") or ""
    vagga = request.args.get("vagga") or ""
    verse_num_input = request.args.get("verse_num") or ""
    limit = request.args.get("limit", default=25, type=int)
    verse_num = None
    if verse_num_input:
        try:
            parsed = int(verse_num_input)
            if parsed > 0:
                verse_num = parsed - 1
        except ValueError:
            verse_num_input = ""

    verses = SuttaVerse.objects.search_verses(
        nikaya=nikaya or None,
        book_number=book_number or None,
        vagga=vagga or None,
        verse_num=verse_num,
        limit=limit or 25,
    )
    facets = {
        "nikayas": list_nikayas(),
        "book_numbers": list_book_numbers(nikaya=nikaya or None),
        "vaggas": list_vaggas(nikaya=nikaya or None, book_number=book_number or None),
    }
    return render_template(
        "verse_browser.html",
        filters={
            "nikaya": nikaya,
            "book_number": book_number,
            "vagga": vagga,
            "verse_num": verse_num_input,
            "limit": limit or 25,
        },
        facets=facets,
        verses=verses,
    )


@app.route("/api/sutta/<string:identifier>/<int:verse_num>")
def sutta_verse(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)

    payload = {
        "identifier": verse.identifier,
        "verse_num": verse.verse_num,
        "text": verse.text,
        "text_hash": verse.text_hash,
    }
    for key in ("nikaya", "vagga", "book_number", "translator", "title", "subtitle"):
        value = getattr(verse, key, None)
        if value not in (None, ""):
            payload[key] = value
    return jsonify(payload)


@app.get("/api/verses/facets")
def verse_facets():
    nikaya = request.args.get("nikaya") or None
    book_number = request.args.get("book_number") or None
    return jsonify({
        "book_numbers": list_book_numbers(nikaya=nikaya),
        "vaggas": list_vaggas(nikaya=nikaya, book_number=book_number),
    })


@app.post("/api/facets/context")
def facet_context():
    data = request.get_json(force=True, silent=True) or {}
    label_terms = {}
    for key in ("person", "gpe", "loc"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            label_terms[key.upper()] = [value.strip()]
        elif isinstance(value, list):
            label_terms[key.upper()] = [item for item in value if isinstance(item, str)]
    if not label_terms:
        return jsonify({
            "ok": True,
            "facets": {
                "PERSON": db.list_entities_by_label("PERSON"),
                "GPE": db.list_entities_by_label("GPE"),
                "LOC": db.list_entities_by_label("LOC"),
            },
        })
    return jsonify({"ok": True, "facets": db.facet_context(label_terms=label_terms)})


@app.post("/api/facets/verses")
def facet_verses():
    data = request.get_json(force=True, silent=True) or {}
    label_terms = {}
    for key in ("person", "gpe", "loc"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            label_terms[key.upper()] = [value.strip()]
        elif isinstance(value, list):
            label_terms[key.upper()] = [item for item in value if isinstance(item, str)]
    limit = data.get("limit", 50)
    items = db.facet_verses(label_terms=label_terms, limit=limit)
    return jsonify({"ok": True, "count": len(items), "items": items})


@app.route("/predict/verse/<string:identifier>/<int:verse_num>")
def predict_verse(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)
    text_value = verse.text or ""
    existing = get_ner_verse_spans(identifier) or []
    spans = []
    for entry in existing:
        if not isinstance(entry, dict):
            continue
        if entry.get("verse_num") != verse_num:
            continue
        try:
            start = int(entry.get("start", 0))
            end = int(entry.get("end", start))
        except (TypeError, ValueError):
            continue
        start = max(0, min(start, len(text_value)))
        end = max(start, min(end, len(text_value)))
        label = entry.get("label") or ""
        text = entry.get("text") or text_value[start:end]
        spans.append({
            "start": start,
            "end": end,
            "label": label,
            "text": text,
        })

    meta = {
        "identifier": verse.identifier,
        "verse_num": verse.verse_num,
    }
    for key in ("nikaya", "vagga", "book_number", "translator", "title", "subtitle"):
        value = getattr(verse, key, None)
        if value not in (None, ""):
            meta[key] = value

    initial_doc = {
        "text": text_value,
        "spans": spans,
        "meta": meta,
    }
    return render_template("predict.html", initial_doc=initial_doc)


@app.route("/speaker_span/verse/<string:identifier>/<int:verse_num>")
def speaker_span_verse(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)

    meta = {
        "identifier": verse.identifier,
        "verse_num": verse.verse_num,
    }
    for key in ("nikaya", "vagga", "book_number", "translator", "title", "subtitle"):
        value = getattr(verse, key, None)
        if value not in (None, ""):
            meta[key] = value

    initial_doc = {
        "text": verse.text or "",
        "meta": meta,
    }
    return render_template("speaker_span.html", initial_doc=initial_doc)


@app.post("/api/verse/<string:identifier>/<int:verse_num>/ner")
def save_verse_spans(identifier: str, verse_num: int):
    verse = SuttaVerse.objects.fetch_sutta_verse(identifier, verse_num)
    if not verse:
        abort(404)

    data = request.get_json(force=True, silent=True) or {}
    spans = data.get("spans", [])
    if not isinstance(spans, list):
        return jsonify({"ok": False, "message": "Spans must be a list."}), 400

    text = verse.text or ""
    length = len(text)
    normalized = []
    for idx, span in enumerate(spans, start=1):
        if not isinstance(span, dict):
            return jsonify({"ok": False, "message": f"Span {idx} must be an object."}), 400
        try:
            start = int(span.get("start"))
            end = int(span.get("end"))
        except (TypeError, ValueError):
            return jsonify({"ok": False, "message": f"Span {idx} has invalid offsets."}), 400
        if start < 0 or end < start or end > length:
            return jsonify({"ok": False, "message": f"Span {idx} has out-of-range offsets."}), 400
        label = span.get("label")
        if not isinstance(label, str) or not label.strip():
            return jsonify({"ok": False, "message": f"Span {idx} requires a label."}), 400
        snippet = span.get("text")
        if not isinstance(snippet, str) or not snippet:
            snippet = text[start:end]
        normalized.append({
            "verse_num": verse_num,
            "start": start,
            "end": end,
            "label": label.strip().upper(),
            "text": snippet,
        })

    success = update_ner_verse_spans(identifier, verse_num, normalized)
    if not success:
        abort(404)
    return jsonify({"ok": True, "saved": len(normalized), "message": "Saved verse annotations."})
    
if __name__ == "__main__":
    app.run(debug=True)
