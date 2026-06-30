.PHONY: uninstall

# Uninstall targets
uninstall: ## Uninstall the local development deployment
	@echo "Uninstalling Airflow development deployment..."
	@set -e; \
	ns="$(NAMESPACE)"; \
	k="$(KUBECTL_DEV)"; \
	$(HELM_DEV) uninstall $(RELEASE_NAME) --namespace $$ns 2>/dev/null || true; \
	echo "Cleaning up remaining resources in $$ns..."; \
	if ! $$k get namespace "$$ns" >/dev/null 2>&1; then \
		echo "Namespace $$ns does not exist, nothing to clean."; \
	else \
		echo "Stopping cron jobs..."; \
		$$k delete cronjobs --all -n "$$ns" --ignore-not-found --force --grace-period=0 2>/dev/null || true; \
		echo "Deleting jobs..."; \
		$$k delete jobs --all -n "$$ns" --ignore-not-found --force --grace-period=0 2>/dev/null || true; \
		echo "Deleting statefulsets..."; \
		$$k delete statefulsets --all -n "$$ns" --ignore-not-found --force --grace-period=0 2>/dev/null || true; \
		echo "Deleting deployments..."; \
		$$k delete deployments --all -n "$$ns" --ignore-not-found --force --grace-period=0 2>/dev/null || true; \
		echo "Deleting replica sets..."; \
		$$k delete replicasets --all -n "$$ns" --ignore-not-found --force --grace-period=0 2>/dev/null || true; \
		echo "Deleting services..."; \
		$$k delete services --all -n "$$ns" --ignore-not-found 2>/dev/null || true; \
		echo "Force deleting pods..."; \
		$$k delete pods --all -n "$$ns" --ignore-not-found --force --grace-period=0 2>/dev/null || true; \
		echo "Removing finalizers from stuck pods..."; \
		for pod in $$($$k get pods -n "$$ns" -o jsonpath='{range .items[?(@.metadata.deletionTimestamp)]}{.metadata.name}{"\n"}{end}' 2>/dev/null); do \
			[ -n "$$pod" ] || continue; \
			$$k patch pod "$$pod" -n "$$ns" -p '{"metadata":{"finalizers":null}}' --type=merge 2>/dev/null || true; \
		done; \
		echo "✅ Namespace workload cleanup complete"; \
	fi
	@echo "⚠️  Note: Persistent volumes are NOT deleted. To delete them:"
	@echo "  $(KUBECTL_DEV) delete pvc --all -n $(NAMESPACE)"
