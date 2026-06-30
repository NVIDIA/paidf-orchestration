KUBECTL_DEV = kubectl
HELM_DEV = helm

# NFS storage (override for non-default clusters/nodes)
NFS_NAMESPACE ?= nfs-system
NFS_STORAGE_CLASS ?= nfs
NFS_EXPORT_PATH ?= /srv/nfs/k8s
NFS_NODE_HOSTNAME ?=
NFS_SERVER ?=
NFS_PROVISIONER_NAME ?= cluster.local/nfs-subdir-external-provisioner
NFS_DIR = $(CURDIR)/nfs

# Configuration
NAMESPACE = sdg-workflow
RELEASE_NAME = sdg-workflow-controller
CHART_PATH = deploy
VALUES_DEV = deploy/values.yaml
SECRET_VALUES_DEV = deploy/secret_values_dev.yaml
SECRETS_ENV = secrets.env
AIRFLOW_PYTHON_DEPS_DIR = $(CURDIR)/.airflow-python-deps
AWS_S3_DAG_BUCKET ?= sdg-workflow-eks-test
PYTHON_REQUIRED_MAJOR = 3
PYTHON_REQUIRED_MINOR = 10
