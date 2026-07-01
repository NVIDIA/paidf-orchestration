.PHONY: sync-dag

sync-dag: ## Package and upload DAGs, plugins, and python deps to S3 (skips unchanged artifacts)
	@echo "=========================================="
	@echo "Syncing Airflow artifacts to S3..."
	@echo "=========================================="
	@set -e; \
	if [ ! -f "$(SECRETS_ENV)" ]; then \
		echo "❌ Error: $(SECRETS_ENV) not found. Run make setup first."; \
		exit 1; \
	fi; \
	set -a; \
	. "./$(SECRETS_ENV)"; \
	set +a; \
	echo ""; \
	echo "Step 1/2: Packaging Airflow runtime dependencies..."; \
	echo "=========================================="; \
	AIRFLOW_PYTHON_DEPS_DIR="$(AIRFLOW_PYTHON_DEPS_DIR)" bash scripts/package_airflow_dependencies.sh; \
	echo ""; \
	echo "Step 2/2: Uploading DAGs, plugins, and dependencies to S3..."; \
	echo "=========================================="; \
	AIRFLOW_PYTHON_DEPS_DIR="$(AIRFLOW_PYTHON_DEPS_DIR)" bash scripts/upload_airflow_artifacts.sh; \
	echo ""; \
	echo "=========================================="; \
	echo "✅ Artifact sync complete!"; \
	echo "=========================================="
