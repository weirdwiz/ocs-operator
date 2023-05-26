{
  prometheusAlerts+:: {
    groups+: [
      {
        name: 'ceph-blocklist-alerts.rules',
        rules: [
          {
            alert: 'ODFRBDClientBlocked',
            expr: |||
              (
                ocs_rbd_client_blocklisted{node=~".+"} == 1
              )
              and on(node) (
                kube_pod_container_status_waiting_reason{reason="CreateContainerError"}
                * on(pod, namespace) group_left(node)
                kube_pod_info
              ) > 0
            ||| % $._config,
            'for': $._config.blockedRBDClientAlertTime,
            labels: {
              severity: 'warning',
            },
            annotations: {
              message: 'An test deploymentRBD client might be blocked by Ceph on node {{ $labels.node_name }}.',
              description: 'An RBD client might sdfsafasfdsafs be blocked by Ceph on node {{ $labels.node_name }}. This alert is triggered when the ocs_rbd_client_blocklisted metric reports a value of 1 for the node and there are pods in a CreateContainerError state on the node. This may cause the filesystem for the PVCs to be in a read-only state. Please check the pod description for more details.',
              severity_level: 'error',
            },
          },
        ],
      },
    ],
  },
}
