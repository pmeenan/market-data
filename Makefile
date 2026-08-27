.PHONY: check format format-check license lint lock sync test

UV ?= $(shell if [ -x .tools/uv ]; then printf '%s' .tools/uv; elif [ -x .venv/bin/uv ]; then printf '%s' .venv/bin/uv; else command -v uv; fi)
UV_RUN := $(UV) run --isolated --locked --extra dev

ifeq ($(strip $(UV)),)
$(error uv not found; run tools/install-uv)
endif

check:
	$(UV_RUN) sh tools/check

sync:
	$(UV) sync --locked --extra dev

lock:
	$(UV) lock --check

lint:
	$(UV_RUN) ruff check .

format-check:
	$(UV_RUN) ruff format --check .

format:
	$(UV_RUN) ruff check --fix .
	$(UV_RUN) ruff format .

test:
	$(UV_RUN) pytest

license:
	$(UV_RUN) python tools/check_licenses.py
