apiVersion: v1
kind: ServiceAccount
metadata:
  name: nfs-subdir-external-provisioner
  namespace: ${NFS_NAMESPACE}
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: nfs-subdir-external-provisioner-runner
rules:
  - apiGroups: [""]
    resources: ["persistentvolumes"]
    verbs: ["get", "list", "watch", "create", "delete"]
  - apiGroups: [""]
    resources: ["persistentvolumeclaims"]
    verbs: ["get", "list", "watch", "update"]
  - apiGroups: ["storage.k8s.io"]
    resources: ["storageclasses"]
    verbs: ["get", "list", "watch"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create", "update", "patch"]
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
  - apiGroups: [""]
    resources: ["secrets"]
    verbs: ["get"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: run-nfs-subdir-external-provisioner
subjects:
  - kind: ServiceAccount
    name: nfs-subdir-external-provisioner
    namespace: ${NFS_NAMESPACE}
roleRef:
  kind: ClusterRole
  name: nfs-subdir-external-provisioner-runner
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: leader-locking-nfs-subdir-external-provisioner
  namespace: ${NFS_NAMESPACE}
rules:
  - apiGroups: [""]
    resources: ["endpoints"]
    verbs: ["get", "list", "watch", "create", "update", "patch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: leader-locking-nfs-subdir-external-provisioner
  namespace: ${NFS_NAMESPACE}
subjects:
  - kind: ServiceAccount
    name: nfs-subdir-external-provisioner
    namespace: ${NFS_NAMESPACE}
roleRef:
  kind: Role
  name: leader-locking-nfs-subdir-external-provisioner
  apiGroup: rbac.authorization.k8s.io
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: nfs-subdir-external-provisioner
  namespace: ${NFS_NAMESPACE}
  labels:
    app: nfs-subdir-external-provisioner
spec:
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: nfs-subdir-external-provisioner
  template:
    metadata:
      labels:
        app: nfs-subdir-external-provisioner
    spec:
      serviceAccountName: nfs-subdir-external-provisioner
      containers:
        - name: nfs-subdir-external-provisioner
          image: ${NFS_PROVISIONER_IMAGE}
          volumeMounts:
            - name: nfs-client-root
              mountPath: /persistentvolumes
          env:
            - name: PROVISIONER_NAME
              value: ${NFS_PROVISIONER_NAME}
            - name: NFS_SERVER
              value: ${NFS_SERVER}
            - name: NFS_PATH
              value: ${NFS_EXPORT_PATH}
      volumes:
        - name: nfs-client-root
          nfs:
            server: ${NFS_SERVER}
            path: ${NFS_EXPORT_PATH}
