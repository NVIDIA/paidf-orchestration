.PHONY: help

help:
	@echo "SDG Workflow Controller - Available Commands"
	@echo ""
	@awk 'BEGIN { FS = ":.*## " } /^[A-Za-z0-9_.-]+:.*## / { printf "  make %-24s - %s\n", $$1, $$2 }' $(MAKEFILE_LIST)
