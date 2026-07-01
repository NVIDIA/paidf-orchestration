.PHONY: install install-sdg-controller nfs sdg-controller

nfs sdg-controller:
	@:

install:
ifeq ($(filter nfs,$(MAKECMDGOALS)),nfs)
	@$(MAKE) install-nfs
else ifeq ($(filter sdg-controller,$(MAKECMDGOALS)),sdg-controller)
	@$(MAKE) install-sdg-controller
else
	@echo "Usage: make install nfs|sdg-controller"
	@echo ""
	@echo "  make install nfs            - Install host NFS and StorageClass"
	@echo "  make install sdg-controller - Package artifacts and deploy SDG controller"
	@exit 1
endif

# Installation/Upgrade target
install-sdg-controller: ## Package runtime dependencies and install/upgrade SDG controller
	@echo "=========================================="
	@echo "Installing/Upgrading for development..."
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
	echo "Step 1/5: Packaging Airflow runtime dependencies..."; \
	echo "=========================================="; \
	AIRFLOW_PYTHON_DEPS_DIR="$(AIRFLOW_PYTHON_DEPS_DIR)" bash scripts/package_airflow_dependencies.sh; \
	echo ""; \
	echo "Step 2/5: Uploading DAGs and dependencies to S3..."; \
	echo "=========================================="; \
	AIRFLOW_PYTHON_DEPS_DIR="$(AIRFLOW_PYTHON_DEPS_DIR)" bash scripts/upload_airflow_artifacts.sh; \
	echo ""; \
	echo "Step 3/5: Updating Helm chart dependencies..."; \
	echo "=========================================="; \
	$(HELM_DEV) dependency update $(CHART_PATH); \
	echo ""; \
	echo "Step 4/5: Uninstalling existing deployment and cleaning up namespace..."; \
	echo "=========================================="; \
	$(MAKE) uninstall; \
	echo ""; \
	echo "Step 5/5: Deploying to Kubernetes..."; \
	echo "=========================================="; \
	$(HELM_DEV) upgrade --install $(RELEASE_NAME) $(CHART_PATH) \
		--namespace $(NAMESPACE) \
		--create-namespace \
		--values $(VALUES_DEV) \
		--values $(SECRET_VALUES_DEV) \
		--wait \
		--timeout 10m; \
	echo ""; \
	echo "=========================================="; \
	echo "✅ Installation complete!"; \
	echo "=========================================="; \
	echo ""; \
	echo "Access the UI:"; \
	echo "  make port-forward"; \
	echo "  Then open http://localhost:8080 (default credentials: admin / admin)"
