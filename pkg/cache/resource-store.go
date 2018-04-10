package cache

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/glog"

	osclient "github.com/openshift/client-go/apps/clientset/versioned"

	svcidler "github.com/openshift/service-idler/pkg/apis/idling/v1alpha2"
	//iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/api/extensions/v1beta1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	kclient "k8s.io/client-go/kubernetes"
	kcache "k8s.io/client-go/tools/cache"
)

const (
	LastSleepTimeAnnotation = "openshift.io/last-sleep-time"
	HibernationLabel        = "openshift.io/hibernate-include"
)

// ResourceObject is a wrapper around PodCache and NamespaceCache, 
// because hibernation needs some custom items added to each of those.
type ResourceObject struct {
	Mutex             sync.RWMutex
	UID               types.UID
	Name              string
	Namespace         string
	Kind              string
	Terminating       bool
	ResourceVersion   string
	RunningTimes      []*RunningTime
	MemoryRequested   resource.Quantity
	DeletionTimestamp time.Time
	Selectors         labels.Selector
	Labels            map[string]string
	Annotations       map[string]string
}


func resourceKey(obj interface{}) (string, error) {
	resObj := obj.(*ResourceObject)
	return string(resObj.UID), nil
}

// ResourceIndexer holds methods for ResourceObject store
type ResourceIndexer interface {
   kcache.Indexer
   UpdateResourceObject(obj *ResourceObject) error
   DeleteResourceObject(obj *ResourceObject) error
   AddResourceObject(obj *ResourceObject) error
}

// resourceStore is a wrapper for pod cache and namespace cache
type resourceStore struct {
	mu sync.RWMutex
	kcache.Indexer
	KubeClient kclient.Interface
}

func (s *resourceStore) NewResourceFromInterface(resource interface{}) (*ResourceObject, error) {
	switch r := resource.(type) {
	case *corev1.Pod:
		return s.NewResourceFromPod(r), nil
	//case *corev1.Namespace:
	//	labels := r.GetLabels()
	//	if include, ok := labels[HibernationLabel]; ok && include == "true" {
	//		return s.NewResourceFromProject(r), nil
	//	} else {
	//		glog.V(2).Infof("Excluding namespace( %s )from hibernation, label( %s=true )not found.", r.Name, HibernationLabel)
	//		return nil, nil
	//	}
	case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
		return nil, nil
	}
	return nil, fmt.Errorf("unknown resource of type %T", resource)
}

// Adds a new runningtime start value to a resource object
func (s *resourceStore) startResource(r *ResourceObject) {
	UID := string(r.UID)
	if !s.ResourceInCache(UID) {
		s.Indexer.Add(r)
	}

	// Make sure object was successfully created before working on it
	obj, exists, err := s.GetResourceByKey(UID)
	if err != nil {
		glog.Errorf("Error: %v", err)
	}

	if exists {
		objCopy := obj.DeepCopy()
		if err != nil {
			glog.Errorf("Couldn't copy resource from cache: %v", err)
			return
		}
		objCopy.Kind = r.Kind
		s.Indexer.Update(objCopy)
		if !objCopy.IsStarted() {
			runTime := &RunningTime{
				Start: time.Now(),
			}
			objCopy.RunningTimes = append(objCopy.RunningTimes, runTime)
			s.Indexer.Update(objCopy)
		}
	} else {
		glog.Errorf("Error starting resource: could not find resource %s %s\n", r.Name, UID)
		return
	}
}

// Adds an end time to a resource object
func (s *resourceStore) stopResource(r *ResourceObject) {
	var stopTime time.Time
	resourceTime := r.DeletionTimestamp
	UID := string(r.UID)

	// See if we already have a reference to this resource in cache
	if s.ResourceInCache(UID) {
		// Use the resource's given deletion time, if it exists
		if !resourceTime.IsZero() {
			stopTime = resourceTime
		}
	} else {
		// If resource is not in cache, then ignore Delete event
		return
	}

	// If there is an error with the given deletion time, use "now"
	now := time.Now()
	if stopTime.After(now) || stopTime.IsZero() {
		stopTime = now
	}

	obj, exists, err := s.GetResourceByKey(UID)
	if err != nil {
		glog.Errorf("Error: %v", err)
	}
	if exists {
		objCopy := obj.DeepCopy()
		if err != nil {
			glog.Errorf("Couldn't copy resource from cache: %v", err)
			return
		}
		runTimeCount := len(objCopy.RunningTimes)

		if objCopy.IsStarted() {
			objCopy.RunningTimes[runTimeCount-1].End = stopTime
			if err := s.Indexer.Update(objCopy); err != nil {
				glog.Errorf("Error: %s", err)
			}
		}
	} else {
		glog.Errorf("Did not find resource %s %s\n", r.Name, UID)
	}
}

func (s *resourceStore) AddOrModify(obj interface{}) error {
	switch r := obj.(type) {
	case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
		return nil
	case *corev1.Pod:
		resObj, err := s.NewResourceFromInterface(obj.(*corev1.Pod))
		if err != nil {
			return err
		}
		if resObj == nil {
			return nil
		}
		glog.V(3).Infof("Received ADD/MODIFY for pod: %s\n", resObj.Name)
		// If the pod is running, make sure it's started in cache
		// Otherwise, make sure it's stopped
		switch r.Status.Phase {
		case corev1.PodRunning:
			s.startResource(resObj)
		case corev1.PodSucceeded, corev1.PodFailed, corev1.PodUnknown:
			s.stopResource(resObj)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) DeleteKapiResource(obj interface{}) error {
	glog.V(3).Infof("Received DELETE event\n")
	switch r := obj.(type) {
	case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
		return nil
	case *corev1.Pod:
		resObj, err := s.NewResourceFromInterface(r)
		if err != nil {
			return err
		}
		if resObj == nil {
			return nil
		}
		UID := string(resObj.UID)
		if s.ResourceInCache(UID) {
			s.stopResource(resObj)
		}
	default:
		glog.V(3).Infof("Object not recognized, Could not delete object")
	}
	return nil
}

func (s *resourceStore) Add(obj interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r := obj.(type) {
	case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
		return nil
	case *corev1.Pod:
		if err := s.AddOrModify(r); err != nil {
			return fmt.Errorf("Error: %s", err)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) Delete(obj interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch obj.(type) {
	case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
		return nil
	case *corev1.Pod:
		if err := s.DeleteKapiResource(obj); err != nil {
			return fmt.Errorf("Error: %s", err)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) Update(obj interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch r := obj.(type) {
	case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
		return nil
	case *corev1.Pod:
		if err := s.AddOrModify(r); err != nil {
			return fmt.Errorf("Error: %s", err)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) UpdateResourceObject(obj *ResourceObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Indexer.Update(obj)
}

func (s *resourceStore) AddResourceObject(obj *ResourceObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Indexer.Add(obj)
}

func (s *resourceStore) DeleteResourceObject(obj *ResourceObject) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch obj.Kind {
	case PodKind:
		UID := string(obj.UID)
		if s.ResourceInCache(UID) {
			// Should this be stopResource?
			s.Indexer.Delete(obj)
		}
	default:
		glog.Errorf("Object not recognized")
	}
	return nil
}

func (s *resourceStore) List() []interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.List()
}

func (s *resourceStore) ListKeys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.ListKeys()
}

func (s *resourceStore) Get(obj interface{}) (item interface{}, exists bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.Get(obj)
}

func (s *resourceStore) GetByKey(key string) (item interface{}, exists bool, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.GetByKey(key)
}

func (s *resourceStore) Replace(objs []interface{}, resVer string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(objs) == 0 {
		return fmt.Errorf("cannot handle situation when replace is called with empty slice")
	}

	accessor, err := meta.TypeAccessor(objs[0])
	if err != nil {
		return err
	}

	listKind := accessor.GetKind()

	var objsToSave []interface{}

	// TODO: if we receive an empty list, what do we do?
	kinds := s.Indexer.ListIndexFuncValues("ofKind")
	for _, kind := range kinds {
		if kind == listKind {
			continue
		}

		objsOfKind, err := s.Indexer.ByIndex("ofKind", kind)
		if err != nil {
			return err
		}
		objsToSave = append(objsToSave, objsOfKind...)
	}
	objsToDelete, err := s.Indexer.ByIndex("ofKind", listKind)

	s.Indexer.Replace(objsToSave, resVer)
	for _, obj := range objs {
		switch r := obj.(type) {
		case *corev1.Namespace, *corev1.ReplicationController, *v1beta1.ReplicaSet, *appsv1beta1.StatefulSet, *corev1.Service:
			return nil
		case *corev1.Pod:
			if err := s.AddOrModify(r); err != nil {
				return err
			}
		default:
			glog.Errorf("Object was not recognized")
		}
	}

	for _, obj := range objsToDelete {
		_, exists, err := s.Indexer.Get(obj)
		if err != nil {
			return err
		}
		if !exists {
			// do what stopResource does
			s.Indexer.Add(obj)
			_, nsexists, err := s.Indexer.Get(obj.(*ResourceObject).Namespace)
			if err != nil {
				return err
			}
			if nsexists {
				s.stopResource(obj.(*ResourceObject))
			} else {
				s.Indexer.Delete(obj)
			}
		}
	}

	return nil
}

func (s *resourceStore) Resync() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Indexer.Resync()
}

func (s *resourceStore) ByIndex(indexName, indexKey string) ([]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.ByIndex(indexName, indexKey)
}

func (s *resourceStore) Index(indexName string, obj interface{}) ([]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.Index(indexName, obj)
}

func (s *resourceStore) ListIndexFuncValues(indexName string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.ListIndexFuncValues(indexName)
}

func (s *resourceStore) GetIndexers() kcache.Indexers {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.GetIndexers()
}

func (s *resourceStore) AddIndexers(newIndexers kcache.Indexers) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.Indexer.AddIndexers(newIndexers)
}

// NewResourceStore creates an Indexer store with the given key function
func NewResourceStore(kubeClient kclient.Interface) *resourceStore {
	store := &resourceStore{
		Indexer: kcache.NewIndexer(resourceKey, kcache.Indexers{
			"byNamespace":        indexResourceByNamespace,
			"byNamespaceAndKind": indexResourceByNamespaceAndKind,
			"ofKind":             getAllResourcesOfKind,
		}),
		OsClient:   osClient,
		KubeClient: kubeClient,
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	return store
}

// Keying functions for Indexer
func indexResourceByNamespace(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	return []string{object.Namespace}, nil
}

func getAllResourcesOfKind(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	return []string{object.Kind}, nil
}

func indexResourceByNamespaceAndKind(obj interface{}) ([]string, error) {
	object := obj.(*ResourceObject)
	fullName := object.Namespace + "/" + object.Kind
	return []string{fullName}, nil
}

type RunningTime struct {
	Start time.Time
	End   time.Time
}

// ResourceObject methods

// IsStopped checks to see if a resource (pod) is stopped in our cache
// Ie, the resource's most recent RunningTime has a specified End time
func (r *ResourceObject) IsStopped() bool {
	runtimes := len(r.RunningTimes)
	if runtimes == 0 {
		return true
	} else {
		return !(r.RunningTimes[runtimes-1].End.IsZero())
	}
}

func (r *ResourceObject) IsStarted() bool {
	return !r.IsStopped()
}

func (in *ResourceObject) DeepCopyInto(out *ResourceObject) {
	*out = *in
	out.UID = in.UID
	out.Name = in.Name
	out.Namespace = in.Namespace
	out.Kind = in.Kind
	out.Terminating = in.Terminating
	out.ResourceVersion = in.ResourceVersion
	out.RunningTimes = in.RunningTimes
	out.MemoryRequested = in.MemoryRequested
	out.LastSleepTime = in.LastSleepTime
	out.DeletionTimestamp = in.DeletionTimestamp
	out.Selectors = in.Selectors
	out.Labels = in.Labels
	out.Annotations = in.Annotations
	return
}

func (in *ResourceObject) DeepCopy() *ResourceObject {
	if in == nil {
		return nil
	}
	out := new(ResourceObject)
	in.DeepCopyInto(out)
	return out
}

func (r *ResourceObject) GetResourceRuntime(period time.Duration) (time.Duration, bool) {
	var total time.Duration
	count := len(r.RunningTimes) - 1
	outsidePeriod := 0

	for i := count; i >= 0; i-- {
		if i == count && r.IsStarted() {
			// special case to see if object is currently running
			// if running && startTime > period, then it's been running for period
			if time.Since(r.RunningTimes[i].Start) > period {
				total += period
			} else {
				total += time.Now().Sub(r.RunningTimes[i].Start)
			}
			continue
		}
		if time.Since(r.RunningTimes[i].End) > period {
			// End time is outside period
			outsidePeriod = i
			break
		} else if time.Since(r.RunningTimes[i].Start) > period {
			// Start time is outside period
			total += r.RunningTimes[i].End.Sub(time.Now().Add(-1 * period))
		} else {
			total += r.RunningTimes[i].End.Sub(r.RunningTimes[i].Start)
		}
	}

	// Remove running times outside of period
	changed := false
	r.RunningTimes = r.RunningTimes[outsidePeriod:]
	// Let sync function know if need to update cache with new ResourceObject for removed times
	if len(r.RunningTimes) < count {
		changed = true
	}
	return total, changed
}

func (s *resourceStore) ResourceInCache(UID string) bool {
	_, exists, err := s.Indexer.GetByKey(UID)
	if err != nil {
		return false
	}
	return exists
}

func (s *resourceStore) GetResourceByKey(UID string) (*ResourceObject, bool, error) {
	obj, exists, err := s.Indexer.GetByKey(UID)
	if err != nil {
		return nil, false, fmt.Errorf("Error getting resource by key: %v", err)
	}
	if exists {
		resource := obj.(*ResourceObject)
		return resource, true, nil
	}
	return nil, false, nil
}

func (s *resourceStore) NewResourceFromPod(pod *corev1.Pod) *ResourceObject {
	terminating := false
	if (pod.Spec.RestartPolicy != corev1.RestartPolicyAlways) && (pod.Spec.ActiveDeadlineSeconds != nil) {
		terminating = true
	}
	resource := &ResourceObject{
		UID:             pod.GetUID(),
		Name:            pod.GetName(),
		Namespace:       pod.GetNamespace(),
		Kind:            PodKind,
		Terminating:     terminating,
		MemoryRequested: pod.Spec.Containers[0].Resources.Limits["memory"],
		RunningTimes:    make([]*RunningTime, 0),
		Labels:          pod.GetLabels(),
		Annotations:     pod.GetAnnotations(),
	}
	if pod.ObjectMeta.DeletionTimestamp.IsZero() {
		resource.DeletionTimestamp = time.Time{}
	} else {
		resource.DeletionTimestamp = pod.ObjectMeta.DeletionTimestamp.Time
	}

	return resource
}

//Take proj in cache and place hibernation annotations
//Also, delete proj in cache if it does not have hibernation label
func ProcessHibernationAnnotationsForProject(namespace *corev1.Namespace) {
	projCopy, err := proj.DeepCopy()
	// Parse any LastSleepTime annotation on the namespace
	if namespace.ObjectMeta.Annotations != nil {
		if namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation] != "" {
			glog.V(2).Infof("Caching previously-set LastSleepTime for project %s: %+v", namespace.Name, namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation])
			parsedTime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation])
			if err != nil {
				parsedTime = s.getParsedTimeFromQuotaCreation(namespace)
				glog.Errorf("Error parsing project LastSleepTime annotation on namespace: %v", err)
			}
			// Not sure if this is necessary
			if !parsedTime.IsZero() {
				projCopy.Annotations[IsAsleepAnnotation] == "true"
			}
		}
	}
	// Find any Idlers in the namespace
    //idlers, err := ic.Idlers(namespace.Name).List(metav1.ListOptions{})
    //if err != nil {
    //        glog.Errorf("Error listing Idlers in namespace( %v ): %v", namespace.Name, err)
    //}
	//switch {
	//case len(idlers.Items) > 1:
    //    glog.V(2).Infof("WHY ARE THERE MORE THAN 1 IDLER IN THIS NAMESPACE?")
	//case len(idlers.Items) == 0:
		// Populate and Add an idler to resourceObject, with wantIdle=false
	//case len(idlers.Items) == 1:
	//	glog.V(2).Infof("Idler( %v )detected in project( %s ).", idlers.Items[0], namespace)
	//	resource.Idler = idlers.Items[0]
	//}
	return resource
}

func (s *resourceStore) getParsedTimeFromQuotaCreation(namespace *corev1.Namespace) time.Time {
	// Try to see if a quota exists and if so, get sleeptime from there
	// If not, delete the annotation from the namespace object
	quotaInterface := s.KubeClient.CoreV1().ResourceQuotas(namespace.Name)
	quota, err := quotaInterface.Get(ProjectSleepQuotaName, metav1.GetOptions{})
	exists := true
	if err != nil {
		if kerrors.IsNotFound(err) {
			exists = false
		} else {
			glog.Errorf("Error getting project resource quota: %v", err)
			return time.Time{}
		}
	}

	if exists {
		parsedTime, err := time.Parse("2006-01-02 15:04:05.999999999 -0700 MST", quota.ObjectMeta.CreationTimestamp.String())
		if err != nil {
			glog.Errorf("Error parsing quota creationtimestamp: %v", err)
		}
		return parsedTime
	} else {
		nsCopy := namespace.DeepCopy()
		delete(nsCopy.Annotations, LastSleepTimeAnnotation)
		_, err = s.KubeClient.CoreV1().Namespaces().Update(nsCopy)
		if err != nil {
			glog.Errorf("Error deleting project LastSleepTime: %v", err)
		}
	}
	return time.Time{}
}

// TODO WRITE THIS
// GetIdlerTargetScalables populates Idler IdlerSpec.TargetScalables
func (c *ProjPodCache) GetIdlerTargetScalables(namespace string) ([]svcidler.CrossGroupObjectReference, error) {
	project, err := c.GetProject(namespace)
	if err != nil {
		return nil, err
	}
	glog.V(2).Infof("Project: %s", project.Name)
	// Store the scalable resources in a map (this will become an annotation on the service later)
	targetScalables := []svcidler.CrossGroupObjectReference{}
	// get some scaleRefs then add them to targetScalables
	//var scaleRefs []svcidler.CrossGroupObjectReference
	return targetScalables, nil
}

// TODO WRITE THIS
// GetIdlerTriggerServiceNames populates Idler IdlerSpec.TriggerServiceNames
func GetIdlerTriggerServiceNames(c *SvcCache, namespace string) ([]string, error) {
	// populate/update Idler.TriggerServiceNames
	return nil, nil
}

