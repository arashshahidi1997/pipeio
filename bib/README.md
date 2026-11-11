# bib/

Bibliography source data. All generated artifacts live under `.projio/`.

## Directory layout

```
bib/
  srcbib/         Zotero / BetterBibTeX exports (.bib files)
  articles/       PDFs organized as <citekey>/<citekey>.pdf
  derivatives/    Extracted outputs (docling, grobid, openalex)
```

## Pipeline

```
srcbib/*.bib  -->  biblio_merge  -->  .projio/biblio/merged.bib
                                                |
                   pipeio_modkey_bib  -->  .projio/pipeio/modkey.bib
                                                |
                                       biblio_compile
                                                |
                                                v
                                   .projio/render/compiled.bib
```

`compiled.bib` is the single bibliography consumed by pandoc and mkdocs-bibtex.

## Commands

### MCP tools (agents)

```
biblio_merge()          Merge srcbib/*.bib --> .projio/biblio/merged.bib
biblio_compile()        Compile all bib_sources --> .projio/render/compiled.bib
biblio_ingest(doi)      Add a paper by DOI
biblio_pdf_fetch(key)   Fetch PDF for a citekey
biblio_docling(key)     Extract fulltext from PDF
biblio_library_quality()  Audit library completeness
citekey_resolve(keys)   Look up citekey metadata
```

### CLI (humans)

```bash
biblio bibtex merge             # same as biblio_merge()
biblio citekeys status          # show configured vs available citekeys
biblio docling <citekey>        # extract fulltext from PDF
biblio rag sync                 # update RAG index config
```

## Configuration

- Merge config: `.projio/biblio/biblio.yml` (under `bibtex.merge`)
- Render config: `.projio/render.yml` (defines `bib_sources` and `bibliography`)
- Active citekeys: derived automatically from the merged bibliography
