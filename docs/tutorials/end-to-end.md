# End-to-End: From Idea to Production

This tutorial walks through the complete pipeio lifecycle — from scaffolding a flow to publishing documentation. It demonstrates every entity in the ontology and shows how pipeio integrates with biblio, codio, and notio.

We'll build a simple `text_analysis` flow that processes text files, detects patterns, and produces summary statistics. No external dependencies — everything runs with Python stdlib.

## 1. Scaffold the flow

```bash
pipeio flow new text_analysis
```

This creates (idempotent — safe to re-run on existing flows):

```
code/pipelines/text_analysis/
├── Snakefile
├── config.yml              # input_manifest, output_manifest, registry
├── publish.yml             # dag: true, scripts: true
├── Makefile
├── rules/                  # for per-mod .smk files
├── scripts/
├── docs/
│   └── index.md
└── notebooks/
    ├── notebook.yml
    ├── explore/.src/       # exploratory notebooks
    └── demo/.src/          # demo notebooks
```

## 2. Configure I/O

Edit `config.yml` or use MCP:

```python
pipeio_config_init(
    flow="text_analysis",
    input_dir="raw",
    output_dir="derivatives/text_analysis",
)
```

Then add output registry groups:

```python
pipeio_config_patch(
    flow="text_analysis",
    registry_entry={
        "wordcount": {
            "bids": {"root": "wordcount"},
            "members": {
                "json": {"suffix": "wordcount", "extension": ".json"},
                "summary": {"suffix": "wordcount", "extension": ".summary.txt"},
            },
        }
    },
    apply=True,
)
```

The resulting `config.yml`:

```yaml
input_dir: raw
output_dir: derivatives/text_analysis
input_manifest: ""
output_manifest: derivatives/text_analysis/manifest.yml

registry:
  wordcount:
    bids:
      root: wordcount
    members:
      json: {suffix: wordcount, extension: .json}
      summary: {suffix: wordcount, extension: .summary.txt}
```

## 3. Create a mod with documentation

```python
pipeio_mod_create(
    flow="text_analysis",
    mod="wordcount",
    description="Count word frequencies in text files",
    inputs={"text": "input text file (.txt)"},
    outputs={
        "json": "word frequency dict",
        "summary": "human-readable summary",
    },
)
```

This creates three things:

**`scripts/wordcount.py`** — script skeleton with I/O unpacking:
```python
"""Count word frequencies in text files"""
from pathlib import Path

def main(snakemake):
    # --- Inputs ---
    text = Path(snakemake.input.text)  # input text file (.txt)
    # --- Outputs ---
    json = Path(snakemake.output.json)  # word frequency dict
    summary = Path(snakemake.output.summary)  # human-readable summary
    # --- Processing (TODO: implement) ---
    json.parent.mkdir(parents=True, exist_ok=True)
    pass

if __name__ == "__main__":
    main(snakemake)
```

**`docs/wordcount/theory.md`** — scientific rationale stub:
```markdown
# Wordcount
## Rationale
<!-- Scientific rationale. Use [@citekey] citations. -->
## References
```

**`docs/wordcount/spec.md`** — technical specification stub.

### Fill in theory.md (biblio integration)

Search the literature for relevant methods:

```python
rag_query(corpus="bib", query="word frequency analysis text mining")
citekey_resolve(citekey="zipf1949")
```

Edit `theory.md` with findings and `[@citekey]` pandoc citations.

### Regenerate spec.md from code

After implementing the script, refresh the spec:

```python
pipeio_mod_doc_refresh(flow="text_analysis", mod="wordcount", facet="spec", apply=True)
```

This auto-generates the I/O contract table, parameter list, and script index from the current code.

## 4. Implement the script

Edit `scripts/wordcount.py` — replace the `pass` with actual logic:

```python
import json as json_mod
from collections import Counter

def main(snakemake):
    text = Path(snakemake.input.text)
    json_out = Path(snakemake.output.json)
    summary_out = Path(snakemake.output.summary)

    words = text.read_text().lower().split()
    counts = Counter(words)

    json_out.parent.mkdir(parents=True, exist_ok=True)
    json_out.write_text(json_mod.dumps(dict(counts.most_common(100))))
    summary_out.write_text(
        f"Total words: {len(words)}\n"
        f"Unique words: {len(counts)}\n"
        f"Top 10: {counts.most_common(10)}\n"
    )
```

## 5. Wire the Snakemake rule

Generate a rule stub:

```python
pipeio_rule_stub(
    flow="text_analysis",
    rule_name="wordcount",
    inputs={"text": 'bids(root="raw", suffix="text", extension=".txt")'},
    outputs={
        "json": {"root": "wordcount", "suffix": "wordcount", "extension": ".json"},
        "summary": {"root": "wordcount", "suffix": "wordcount", "extension": ".summary.txt"},
    },
    script="scripts/wordcount.py",
)
```

Review the generated rule, then insert:

```python
pipeio_rule_insert(
    flow="text_analysis",
    rule_name="wordcount",
    rule_text="<the generated rule text>",
)
```

## 6. Create an exploration notebook

```python
pipeio_nb_create(
    flow="text_analysis",
    name="investigate_distributions",
    kind="investigate",
    mod="wordcount",
    description="Explore word frequency distributions across subjects",
)
```

This creates `notebooks/explore/.src/investigate_distributions.py` with:
- Jupytext header
- Config loading cell (reads `config.yml`, lists registry groups)
- Data loading section with subject iteration pattern
- Analysis section
- Findings section (feeds into `theory.md`)

The notebook goes to `explore/` because `kind="investigate"`. It's never published.

## 7. Add a second script to the mod

Need another processing step? Use `script_create`:

```python
pipeio_script_create(
    flow="text_analysis",
    mod="wordcount",
    script_name="wordcount_plot",
    description="Generate word cloud visualization",
    inputs={"json": "word frequency JSON"},
    outputs={"png": "word cloud image"},
)
```

This creates `scripts/wordcount_plot.py` with the same I/O template pattern.

## 8. Validate

```python
pipeio_mod_audit(flow="text_analysis", mod="wordcount")
```

Checks:
- All registered rules exist in the Snakefile
- All `script:` references point to existing files
- `docs/wordcount/theory.md` and `spec.md` exist and are non-empty
- Rule names follow `wordcount_*` prefix convention

Also validate contracts:

```python
pipeio_contracts_validate()
```

## 9. Promote notebook findings

Once the investigation notebook has proven the approach:

```python
pipeio_nb_promote(
    flow="text_analysis",
    name="investigate_distributions",
    mod="distribution_analysis",
    description="Analyze word frequency distributions",
    apply=True,
)
```

This:
1. Analyzes the notebook (imports, sections, cells)
2. Creates `scripts/distribution_analysis.py` with extracted imports
3. Returns a rule stub for review
4. Creates `docs/distribution_analysis/theory.md` + `spec.md`

## 10. Create a demo notebook

```python
pipeio_nb_create(
    flow="text_analysis",
    name="demo_wordcount",
    kind="demo",
    mod="wordcount",
    description="Showcase word frequency analysis results",
)
```

This creates `notebooks/demo/.src/demo_wordcount.py` with a demo template:
- Load final outputs section
- Visualization section
- Summary section

Demo notebooks are published to the project site as HTML.

## 11. Publish documentation

Configure what to publish in `publish.yml`:

```yaml
dag: true          # publish the rule dependency graph
report: false
scripts: true      # generate script index with git links
```

Collect and publish:

```python
pipeio_docs_collect()    # collects docs, notebooks, DAG, scripts → docs/pipelines/text_analysis/
pipeio_docs_nav()        # generates MkDocs nav fragment
```

Published structure:

```
docs/pipelines/text_analysis/
├── index.md                      # flow overview
├── dag.svg                       # rule dependency graph
├── scripts.md                    # auto-generated script index
├── mods/
│   └── wordcount/
│       ├── theory.md             # scientific rationale + citations
│       └── spec.md               # I/O contracts
└── notebooks/
    └── demo_wordcount.html       # rendered demo notebook
```

## 12. Cross-flow wiring

If another flow needs to consume `text_analysis` outputs:

```yaml
# In downstream flow's config.yml
input_dir: "derivatives/text_analysis"
input_manifest: "derivatives/text_analysis/manifest.yml"
```

Verify the chain:

```python
pipeio_cross_flow()      # maps manifest → manifest chains
pipeio_contracts_validate()
```

## Entity summary

| Entity | Tool used | File created |
|--------|-----------|-------------|
| Flow | `flow new` | Snakefile, config.yml, publish.yml, Makefile |
| Config | `config_init`, `config_patch` | config.yml with registry groups |
| Mod | `mod_create` | scripts/wordcount.py, docs/wordcount/theory.md + spec.md |
| Script | `mod_create`, `script_create` | scripts/*.py with I/O template |
| Rule | `rule_stub`, `rule_insert` | Rule block in Snakefile |
| Notebook (explore) | `nb_create(kind="investigate")` | notebooks/explore/.src/*.py |
| Notebook (demo) | `nb_create(kind="demo")` | notebooks/demo/.src/*.py |
| Mod docs | `mod_create`, `mod_doc_refresh` | docs/{mod}/theory.md, spec.md |
| Published docs | `docs_collect` | docs/pipelines/{flow}/ |
| Promotion | `nb_promote` | Script + rule stub from notebook |
| Audit | `mod_audit`, `nb_audit`, `contracts_validate` | Structured findings |
