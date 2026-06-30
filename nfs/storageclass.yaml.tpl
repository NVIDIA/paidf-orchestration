apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: ${NFS_STORAGE_CLASS}
  annotations:
    storageclass.kubernetes.io/is-default-class: "false"
provisioner: ${NFS_PROVISIONER_NAME}
parameters:
  archiveOnDelete: "false"
reclaimPolicy: Delete
volumeBindingMode: Immediate
allowVolumeExpansion: true
