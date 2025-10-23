PY := python
SCRIPTS := ne-data/scripts
REPO_ROOT := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
WORK    := $(REPO_ROOT)/work
SCRIPTS := $(REPO_ROOT)/scripts
MODEL   := ne-data/models/1022_model/model-best/
PATTERNS := $(REPO_ROOT)/patterns/entity_ruler/patterns.jsonl

TRAIN_JSONL := $(WORK)/train.jsonl
DEV_JSONL := $(WORK)/dev.jsonl

.PHONY: help sanity train eval clean

help:
	@echo "make sanity    # quick checks on data & patterns"
	@echo "make train     # trains model_minimal using train_minimal.py"
	@echo "make eval      # quick eval on pasted text (stdin)"
	@echo "make clean     # remove model_minimal"

sanity:
	@echo "== patterns count ==" ; \
	if [ -f "$(PATTERNS)" ]; then wc -l "$(PATTERNS)"; else echo "MISSING: $(PATTERNS)"; fi
	@echo "== jsonl presence ==" ; \
	for f in "$(TRAIN_JSONL)" "$(DEV_JSONL)"; do \
	  if [ -f $$f ]; then echo "OK  - $$f"; else echo "MISS- $$f"; fi; \
	done

train:
	@echo "== training to $(MODEL) =="
	$(PY) $(SCRIPTS)/train_minimal.py

# one-liner eval (no heredoc; friendly to macOS make)
eval:
	@echo "== paste text, then Ctrl-D =="
	@$(PY) -c 'import spacy,sys,pathlib; m=pathlib.Path("$(MODEL)"); \
	nlp=spacy.load(m.as_posix()); txt=sys.stdin.read(); \
	print([(e.text,e.label_) for e in nlp(txt).ents])'

clean:
	rm -rf "$(MODEL)"
