package forcesleep

import (
	"fmt"
	"strconv"
	"github.com/openshift/online-hibernation/pkg/cache"

	"k8s.io/apimachinery/pkg/api/resource"
	corev1 "k8s.io/api/core/v1"
)

// Custom sorting for prioritizing projects in force-sleep
type Projects []interface{}

func (p Projects) Len() int {
	return len(p)
}
func (p Projects) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p Projects) Less(i, j int) (bool, error) {
	p1 := p[i].(*corev1.Namespace)
	p2 := p[j].(*corev1.Namespace)
	p1int, err := strconv.Atoi(p1.Annotations[ProjectSortIndexAnnotation])
	if err != nil {
		return false, fmt.Errorf("Force-sleeper: %v", err)
	}
	p2int, err := strconv.Atoi(p2.Annotations[ProjectSortIndexAnnotation])
	if err != nil {
		return false, fmt.Errorf("Force-sleeper: %v", err)
	}
	return p1int < p2int, nil
}

func getQuotaSeconds(seconds float64, request, limit resource.Quantity) float64 {
	requestVal := float64(request.Value())
	limitVal := float64(limit.Value())
	var percentage float64
	percentage = requestVal / limitVal
	return seconds * percentage
}
