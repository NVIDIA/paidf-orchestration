# SDG Workflow Controller - Makefile
# Convenience commands for managing the Airflow deployment

.DEFAULT_GOAL := help

include make/common.mk
include $(filter-out make/common.mk,$(wildcard make/*.mk))
