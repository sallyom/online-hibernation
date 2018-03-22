package idling

import (
	"fmt"
	"strconv"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"
	oidler "github.com/openshift/origin-idler/pkg/apis/idling/v1alpha2"

	appsv1 "github.com/openshift/api/apps/v1"
	"k8s.io/api/extensions/v1beta1"

	"github.com/golang/glog"
	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// GetIdlerTargetScalables populates Idler IdlerSpec.TargetScalables
func GetIdlerTargetScalables(c *cache.Cache, namespace string) ([]oidler.CrossGroupObjectReference, error) {
	project, err := c.GetProject(namespace)
	if err != nil {
		return nil, err
	}
		// Store the scalable resources in a map (this will become an annotation on the service later)
		targetScalables := []oidler.CrossGroupObjectReference{}
		unidledScales := []oidler.UnidleInfo
		// get some scaleRefs then add them to targetScalables
	    var scaleRefs []oidler.CrossGroupObjectReference
		for _, scaleRef := range scaleRefs {
			scaleRefPlusPreviousScale, err := someFunctoGetThat(scaleRef)
			if err != nil {
				return nil, err
			}
			// Update UnidleInfo here, also...for each scaleRef, get PreviousScale, then
			// create UnidleInfo{ CrossGroupObjectReference, PreviousScale }
			// then add that to IdlerStatus.UnidledScales
			targetScalables = append(targetScalables, scaleRef)
			unidledScales = append(unidledScales, scaleRefPlusPreviousScale)
		}
	return targetScalables, nil
}

// SomeFuncToGetThat returns the oidlerUnidleInfo struct
func SomeFuncToGetThat(oidler.CrossGroupObjectReference)(oidler.UnidleInfo, err) {
	return nil, nil
}

// GetIdlerTriggerServiceNames populates Idler IdlerSpec.TriggerServiceNames
func GetIdlerTriggerServiceNames(c *cache.Cache, namespace string) ([]string, error) {
	// populate/update Idler.TriggerServiceNames
	return nil, nil
}

// IdleProject idles all scalables in a given namespace
func IdleProject(c *cache.Cache, namespace string) error {
	project, err := c.GetProject(namespace)
	if err != nil {
		return err
	}
	// Something like
	idlerInterface := c.IdlerClient.Idlers(namespace)
	newIdler := project.Idler.DeepCopy()
	newIdler.Spec.WantIdle = true
	_, err = idlerInterface.Update(newIdler)
	if err != nil {
		return err
	}
	return nil
}

// UnidleProject unidles all scalables in a given namespace
func UnidleProject(c *cache.Cache, namespace string) error {
	project, err := c.GetProject(namespace)
	if err != nil {
		return err
	}
	// Something like
	idlerInterface := c.IdlerClient.Idlers(namespace)
	newIdler := project.Idler.DeepCopy()
	newIdler.Spec.WantIdle = false
	_, err = idlerInterface.Update(newIdler)
	if err != nil {
		return err
	}
	return nil
}

