.PHONY: setup

setup: ## Validate secrets and generate Helm secret values for deployment
	@echo "Setting up development environment..."
	@command -v kubectl >/dev/null 2>&1 || { echo "❌ Error: kubectl is required. Install kubectl and retry."; exit 1; }
	@command -v helm >/dev/null 2>&1 || { echo "❌ Error: helm is required. Install Helm 3 and retry."; exit 1; }
	@echo ""
	@echo "Setting up secrets for Kubernetes deployment..."
	@echo "Uses secrets.env for credentials."
	@echo "Setup fails if required secrets are missing."
	@echo ""
	@bash scripts/setup_airflow_secrets.sh
	@echo ""
	@echo "✅ Kubernetes secret values written to deploy/secret_values_dev.yaml"
	@echo "=========================================="
	@echo "✅ Complete development environment setup finished!"
	@echo "=========================================="
	@echo ""
	@echo "Next steps:"
	@echo "  - Run 'make install sdg-controller' to deploy SDG controller to Kubernetes"
	@echo ""
