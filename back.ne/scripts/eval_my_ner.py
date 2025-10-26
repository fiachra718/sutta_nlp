#!/usr/bin/env python3
import sys, argparse, json
import spacy

def iter_inputs(path_arg: str | None):
    """Yield non-empty lines from file path, '-' (stdin), or stdin if no path."""
    if path_arg and path_arg != "-":
        with open(path_arg, "r", encoding="utf-8") as f:
            for line in f:
                line = line.rstrip("\n")
                if line.strip():
                    yield line
    else:
        # read from stdin
        for line in sys.stdin:
            line = line.rstrip("\n")
            if line.strip():
                yield line

def main():
    p = argparse.ArgumentParser(
        description="NER eval/preview: reads lines from FILE or stdin and prints entities."
    )
    p.add_argument("FILE", nargs="?", help="Text file (one example per line). Use '-' or omit to read stdin.")
    p.add_argument("-m","--model", required=True, help="Path to trained spaCy model (e.g., ./model_out/model-best)")
    p.add_argument("-p","--patterns", help="Optional EntityRuler patterns.jsonl to load before NER")
    p.add_argument("--no-overwrite", action="store_true",
                   help="If set, rules will NOT overwrite model predictions (default: overwrite).")
    p.add_argument("--labels", nargs="*", default=None,
                   help="Only show these labels (e.g., PERSON GPE LOC NORP).")
    p.add_argument("--jsonl", action="store_true", help="Emit JSONL (one object per line).")
    p.add_argument("--batch-size", type=int, default=64, help="spaCy pipe batch size (default: 64)")
    args = p.parse_args()

    nlp = spacy.load(args.model)

    # Optional rules at inference
    if args.patterns:
        cfg = {"overwrite_ents": (not args.no_overwrite)}
        ruler = nlp.add_pipe("entity_ruler", before="ner", config=cfg)
        ruler.from_disk(args.patterns)

    texts = list(iter_inputs(args.FILE))
    if not texts:
        sys.exit(0)

    def ents_view(doc):
        ents = [(ent.text, ent.label_) for ent in doc.ents]
        if args.labels:
            ents = [e for e in ents if e[1] in set(args.labels)]
        return ents

    for doc in nlp.pipe(texts, batch_size=args.batch_size):
        if args.jsonl:
            print(json.dumps({"text": doc.text, "ents": [(e.text, e.label_) for e in doc.ents]}, ensure_ascii=False))
        else:
            ents = ents_view(doc)
            print(f"TEXT: {doc.text}")
            print(f"ENTS: {ents}")
            print("-" * 40)

if __name__ == "__main__":
    main()
