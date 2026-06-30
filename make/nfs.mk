.PHONY: install-nfs

install-nfs: ## Install host NFS export and StorageClass on the cluster
	@NFS_NAMESPACE="$(NFS_NAMESPACE)" \
	NFS_STORAGE_CLASS="$(NFS_STORAGE_CLASS)" \
	NFS_EXPORT_PATH="$(NFS_EXPORT_PATH)" \
	NFS_NODE_HOSTNAME="$(NFS_NODE_HOSTNAME)" \
	NFS_SERVER="$(NFS_SERVER)" \
	NFS_PROVISIONER_NAME="$(NFS_PROVISIONER_NAME)" \
	KUBECTL="$(KUBECTL_DEV)" \
	bash scripts/install_nfs.sh
