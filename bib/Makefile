SHELL := /bin/bash

.PHONY: help docling-all docling-one citekeys-list citekeys-status ui-serve site-build site-serve site-clean site-doctor

help:
	@echo "biblio (Docling) helper targets"
	@echo ""
	@echo "Targets:"
	@echo "  docling-all      Run Docling for all citekeys"
	@echo "  docling-one      Run Docling for one key (make docling-one key=...)"
	@echo "  citekeys-list    Print configured citekeys"
	@echo "  citekeys-status  Show discovered papers and whether they are configured"
	@echo "  ui-serve         Serve the interactive bibliography UI"
	@echo "  site-build       Build the standalone bibliography site"
	@echo "  site-serve       Serve the standalone bibliography site locally"
	@echo "  site-clean       Remove generated bibliography site output"
	@echo "  site-doctor      Report bibliography site readiness"

docling-all:
	biblio docling run --all

docling-one:
	@test -n \"$(key)\" || (echo \"Usage: make docling-one key=<citekey>\"; exit 2)
	biblio docling run --key \"$(key)\"

citekeys-list:
	biblio citekeys list

citekeys-status:
	biblio citekeys status

ui-serve:
	biblio ui serve

site-build:
	biblio site build --force

site-serve:
	biblio site serve

site-clean:
	biblio site clean

site-doctor:
	biblio site doctor
