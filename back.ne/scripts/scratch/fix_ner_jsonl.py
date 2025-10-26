# fix_ner_jsonl.py
import json, argparse, re
import spacy

PUNCT_EDGE = re.compile(r"^[\s\"'“”‘’,.:;!?()-]+|[\s\"'“”‘’,.:;!?()-]+$")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--in", dest="inp", required=True)
    ap.add_argument("--out", dest="out", required=True)
    ap.add_argument("--lang", default="en")
    ap.add_argument("--mode", default="expand", choices=["expand","contract","strict"])
    args = ap.parse_args()

    # tokenizer-only pipeline
    tok = spacy.blank(args.lang)

    kept = 0; fixed = 0; dropped = 0; total_spans = 0; misalign = 0
    with open(args.inp, encoding="utf-8") as f_in, open(args.out, "w", encoding="utf-8") as f_out:
        for line in f_in:
            if not line.strip(): continue
            j = json.loads(line)
            text = j["text"]
            ents = j["entities"]
            total_spans += len(ents)

            doc = tok.make_doc(text)
            new_ents = []
            for (s, e, lbl) in ents:
                # 1) snap to token boundaries
                span = doc.char_span(s, e, label=lbl, alignment_mode=args.mode)
                if not span:
                    misalign += 1
                    continue
                s2, e2 = span.start_char, span.end_char
                # 2) trim leading/trailing punctuation/space (common cause of W030)
                surf = text[s2:e2]
                m = PUNCT_EDGE.sub("", surf)
                if not m:
                    dropped += 1
                    continue
                # compute trimmed offsets
                lead = len(surf) - len(surf.lstrip(" \t\r\n\"'“”‘’,.:;!?()-"))
                tail = len(surf.rstrip(" \t\r\n\"'“”‘’,.:;!?()-"))
                s3 = s2 + lead
                e3 = s2 + tail
                if s3 >= e3:
                    dropped += 1
                    continue
                new_ents.append((s3, e3, lbl))
                fixed += (s3, e3) != (s, e)

            if new_ents:
                kept += 1
                f_out.write(json.dumps({"text": text, "entities": new_ents, **({"meta": j["meta"]} if "meta" in j else {})},
                                       ensure_ascii=False) + "\n")

    print(f"Examples kept : {kept}")
    print(f"Spans total   : {total_spans}")
    print(f"Spans misalign: {misalign}  (dropped during fix)")
    print(f"Spans trimmed : {fixed}")
    print(f"Spans dropped : {dropped}")

if __name__ == "__main__":
    main()