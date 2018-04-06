package idling

import (
	"fmt"
	"strconv"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"
	svcidler "github.com/openshift/service-idler/pkg/apis/idling/v1alpha2"

	appsv1 "github.com/openshift/api/apps/v1"
	"k8s.io/api/extensions/v1beta1"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// UpdateIdler keeps project.Idler up-to-date
func UpdateIdler(c *cache.NamespaceCache, namespace string, wantIdle bool) error {
	idlerInterface := c.IdlerClient.Idlers(namespace)
	//Need FindIdlerForNS method
	updatedIdler := project.Idler.DeepCopy()
	updatedIdler.Spec.WantIdle = wantIdle
	updatedIdler.Spec.TargetScalables, err = getIdlerTargetScalables(c, namespace)
	if err != nil {
		return err
	}
	updatedIdler.Spec.TriggerServiceNames, err = getIdlerTriggerServiceNames(c, namespace)
	if err != nil {
		return err
	}
	_, err = idlerInterface.Update(updatedIdler)
	if err != nil {
		return err
	}
}

// getIdlerTargetScalables populates Idler IdlerSpec.TargetScalables
func getIdlerTargetScalables(c *cache.Cache, namespace string) ([]svcidler.CrossGroupObjectReference, error) {
		// Store the scalable resources in a map (this will become an annotation on the service later)
		targetScalables := []svcidler.CrossGroupObjectReference{}
		// get some scaleRefs then add them to targetScalables
	    var scaleRefs []svcidler.CrossGroupObjectReference
		for _, scaleRef := range scaleRefs {
			scaleRefPlusPreviousScale, err := someFunctoGetThat(scaleRef)
			if err != nil {
				return nil, err
			}
			// Update UnidleInfo here, also...for each scaleRef, get PreviousScale, then
			// create UnidleInfo{ CrossGroupObjectReference, PreviousScale }
			// then add that to IdlerStatus.UnidledScales
			targetScalables = append(targetScalables, scaleRef)
		}
	return targetScalables, nil
}

// getIdlerTriggerServiceNames populates Idler IdlerSpec.TriggerServiceNames
func getIdlerTriggerServiceNames(c *cache.SvcCache, namespace string) ([]string, error) {
	// populate/update Idler.TriggerServiceNames
	return nil, nil
}

// IdleProject idles all scalables in a given namespace
func IdleProject(c *cache.ProjPodCache, namespace string) error {
	project, err := c.GetProject(namespace)
	if err != nil {
		return err
	}
	// Something like
	idlerInterface := c.IdlerClient.Idlers(namespace)
	updatedIdler := project.Idler.DeepCopy()
	updatedIdler.Spec.WantIdle = true
	_, err = idlerInterface.Update(updatedIdler)
	if err != nil {
		return err
	}
	return nil
}

// UnidleProject unidles all scalables in a given namespace
func UnidleProject(c *cache.ProjPodCache, namespace string) error {
	project, err := c.GetProject(namespace)
	if err != nil {
		return err
	}
	// Something like
	idlerInterface := c.IdlerClient.Idlers(namespace)
	updatedIdler := project.Idler.DeepCopy()
	updatedIdler.Spec.WantIdle = false
	_, err = idlerInterface.Update(updatedIdler)
	if err != nil {
		return err
	}
	return nil
}

