package storagecluster

import (
	"context"
	"reflect"

	ocsv1 "github.com/red-hat-storage/ocs-operator/api/v1"
	"github.com/red-hat-storage/ocs-operator/controllers/defaults"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

const (

	// RookCephToolDeploymentName is the name of the rook-ceph-tools deployment
	rookCephToolDeploymentName = "rook-ceph-tools"
)

func (r *StorageClusterReconciler) ensureToolsDeployment(sc *ocsv1.StorageCluster) error {

	reconcileStrategy := ReconcileStrategy(sc.Spec.ManagedResources.CephToolbox.ReconcileStrategy)
	if reconcileStrategy == ReconcileStrategyIgnore {
		return nil
	}
	var isFound bool
	namespace := sc.Namespace

	tolerations := []corev1.Toleration{{
		Key:      defaults.NodeTolerationKey,
		Operator: corev1.TolerationOpEqual,
		Value:    "true",
		Effect:   corev1.TaintEffectNoSchedule,
	}}

	// Get the ocsinitialization CR
	ocsinit := &ocsv1.OCSInitialization{}
	err := r.Client.Get(context.TODO(), types.NamespacedName{Name: "ocsinit", Namespace: namespace}, ocsinit)
	if err != nil && !errors.IsNotFound(err) {
		return err
	}

	// Get tolerations from both the storagecluster CR & ocsinitialization CR
	// Keeping this for ability to add tolerations from ocsinitialization CR
	tolerations = append(tolerations, getPlacement(sc, "toolbox").Tolerations...)
	tolerations = append(tolerations, ocsinit.Spec.Tolerations...)

	//Remove duplicate tolerations
	tolerations = removeDuplicateTolerations(tolerations)

	toolsDeployment := sc.NewToolsDeployment(tolerations)
	foundToolsDeployment := &appsv1.Deployment{}
	err = r.Client.Get(context.TODO(), types.NamespacedName{Name: rookCephToolDeploymentName, Namespace: namespace}, foundToolsDeployment)

	if err == nil {
		isFound = true
	} else if errors.IsNotFound(err) {
		isFound = false
	} else {
		return err
	}

	// Checking both ocsinitialization & storagecluster CR for their Enablecephtools field
	if sc.Spec.EnableCephTools || ocsinit.Spec.EnableCephTools {
		// Create or Update if ceph tools is enabled.

		//Adding Ownerreference to the ceph tools
		err = controllerutil.SetOwnerReference(sc, toolsDeployment, r.Client.Scheme())
		if err != nil {
			return err
		}

		if !isFound {
			return r.Client.Create(context.TODO(), toolsDeployment)
		} else if !reflect.DeepEqual(foundToolsDeployment.Spec, toolsDeployment.Spec) {

			updateDeployment := foundToolsDeployment.DeepCopy()
			updateDeployment.Spec = *toolsDeployment.Spec.DeepCopy()

			return r.Client.Update(context.TODO(), updateDeployment)
		}
	} else if isFound {
		// delete if ceph tools exists and is disabled
		return r.Client.Delete(context.TODO(), foundToolsDeployment)
	}
	return nil
}

func removeDuplicateTolerations(tolerations []corev1.Toleration) []corev1.Toleration {
	keys := make(map[corev1.Toleration]bool)
	list := []corev1.Toleration{}
	for _, entry := range tolerations {
		if _, value := keys[entry]; !value {
			keys[entry] = true
			list = append(list, entry)
		}
	}
	return list
}
