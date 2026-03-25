# projio.mk — shared targets, managed by projio
# Include from your Makefile: -include .projio/projio.mk

PYTHON  ?= python
DATALAD ?= /storage/share/python/environments/Anaconda3/envs/labpy/bin/datalad
MKDOCS  ?= $(PYTHON) -m mkdocs
PROJIO  ?= /storage/share/python/environments/Anaconda3/envs/rag/bin/python -m projio
MSG     ?= Update

.PHONY: save push url
.PHONY: projio-init projio-config-user projio-config-show projio-status projio-auth
.PHONY: projio-gh projio-gl projio-ria site-build site-serve site-stop site-list site-detect mcp mcp-config

# --- DataLad targets ---
save:
	$(DATALAD) save -m "$(MSG)"

push:
	$(DATALAD) push --to github

url:
	$(PROJIO) url -C .

# --- Projio targets ---
projio-init:
	$(PROJIO) init .

projio-config-user:
	$(PROJIO) config init-user

projio-config-show:
	$(PROJIO) config -C . show

projio-status:
	$(PROJIO) status -C .

projio-auth:
	$(PROJIO) auth -C . doctor

projio-gh:
	$(PROJIO) sibling -C . github

projio-gl:
	$(PROJIO) sibling -C . gitlab

projio-ria:
	$(PROJIO) sibling -C . ria

site-build:
	$(PROJIO) site build -C .

site-serve:
	$(PROJIO) site serve -C .

site-stop:
	$(PROJIO) site stop -C . --all

site-list:
	$(PROJIO) site list -C .

site-detect:
	$(PROJIO) site detect -C .

mcp:
	$(PROJIO) mcp -C .

mcp-config:
	$(PROJIO) mcp-config -C . --yes
