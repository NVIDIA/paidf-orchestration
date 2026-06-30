apiVersion: batch/v1
kind: Job
metadata:
  name: setup-host-nfs
  namespace: ${NFS_NAMESPACE}
spec:
  backoffLimit: 2
  template:
    spec:
      hostNetwork: true
      hostPID: true
      restartPolicy: Never
      nodeSelector:
        kubernetes.io/hostname: ${NFS_NODE_HOSTNAME}
      containers:
        - name: setup
          image: ${NFS_SETUP_IMAGE}
          securityContext:
            privileged: true
          command:
            - chroot
            - /host
            - bash
            - -ec
            - |
              export DEBIAN_FRONTEND=noninteractive
              apt-get update
              apt-get install -y nfs-kernel-server rpcbind
              mkdir -p ${NFS_EXPORT_PATH}
              chown nobody:nogroup ${NFS_EXPORT_PATH}
              chmod 0777 ${NFS_EXPORT_PATH}
              grep -q '${NFS_EXPORT_PATH}' /etc/exports || \
                echo '${NFS_EXPORT_PATH} *(rw,sync,no_subtree_check,no_root_squash,insecure)' >> /etc/exports
              exportfs -ra
              systemctl enable rpcbind nfs-server
              systemctl restart rpcbind nfs-server
              showmount -e localhost
          volumeMounts:
            - name: host
              mountPath: /host
      volumes:
        - name: host
          hostPath:
            path: /
