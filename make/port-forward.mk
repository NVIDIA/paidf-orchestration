.PHONY: port-forward

port-forward: ## Forward Airflow UI to http://localhost:8080
	@echo "Forwarding Airflow UI to http://localhost:8080"
	@echo "Press Ctrl+C to stop"
	@echo ""
	@$(KUBECTL_DEV) port-forward -n $(NAMESPACE) --address=0.0.0.0 svc/$(RELEASE_NAME)-api-server 8080:8080
