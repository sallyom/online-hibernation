package cache

import (
	"fmt"

	"github.com/openshift/api/apps/v1"
	osclient "github.com/openshift/client-go/apps/clientset/versioned"
	appsv1 "github.com/openshift/client-go/apps/clientset/versioned/typed/apps/v1"
	iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"
	//svcidler "github.com/openshift/service-idler/pkg/apis/idling/v1alpha2"
	"k8s.io/api/extensions/v1beta1"
	extkclientv1beta1 "k8s.io/client-go/kubernetes/typed/extensions/v1beta1"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	appsv1beta1 "k8s.io/api/apps/v1beta1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	kclient "k8s.io/client-go/kubernetes"
	kclientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
	restclient "k8s.io/client-go/rest"
	kcache "k8s.io/client-go/tools/cache"

	appsscheme "github.com/openshift/client-go/apps/clientset/versioned/scheme"
	serializer "k8s.io/apimachinery/pkg/runtime/serializer"
	kscheme "k8s.io/client-go/kubernetes/scheme"
)

var Scheme = runtime.NewScheme()
var Codecs = serializer.NewCodecFactory(Scheme)

func init() {
       kscheme.AddToScheme(Scheme)
       appsscheme.AddToScheme(Scheme)
}

const (
	PodKind               = "Pod"
	RCKind                = "ReplicationController"
	DCKind                = "DeploymentConfig"
	RSKind                = "ReplicaSet"
	DepKind               = "Deployment"
	ServiceKind           = "Service"
	ProjectKind           = "Namespace"
	ProjectSleepQuotaName = "force-sleep"
	OpenShiftDCName       = "openshift.io/deployment-config.name"
	BuildAnnotation       = "openshift.io/build.name"
)

type PodCache struct {
	Indexer     ResourceIndexer
	KubeClient  kclient.Interface
	Config      *restclient.Config
	RESTMapper  apimeta.RESTMapper
	stopChan    <-chan struct{}
}

func NewPodCache(kubeClient kclient.Interface, config *restclient.Config, mapper apimeta.RESTMapper) *PodCache {
	return &PodCache{
		Indexer:	 NewResourceStore(osclient, kubeClient),
		KubeClient:  kubeClient,
		Config:      config,
		RESTMapper:  mapper,
	}
}

func (podc *PodCache) Run(stopChan <-chan struct{}) {
	podc.stopChan = stopChan
	podLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return projpodc.KubeClient.CoreV1().Pods(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return projpodc.KubeClient.CoreV1().Pods(metav1.NamespaceAll).Watch(options)
		},
	}
	go kcache.NewReflector(podLW, &corev1.Pod{}, podc.Indexer, 0).Run(podc.stopChan)

	podc.stopChan = stopChan
}

type ProjectCache struct {
	OsClient    osclient.Interface
	KubeClient  kclient.Interface
	Config      *restclient.Config
	RESTMapper  apimeta.RESTMapper
	stopChan    <-chan struct{}
}

func NewProjectCache(kubeClient kclient.Interface,  config *restclient.Config, mapper apimeta.RESTMapper) *ProjectCache {
	return &ProjectCache{
		KubeClient:  kubeClient,
		Config:      config,
		RESTMapper:  mapper,
	}
}

func (projc *ProjectCache) Run(stopChan <-chan struct{}) {
	projc.stopChan = stopChan
	projStore := kcache.NewStore(kcache.MetaNamespaceKeyFunc)
	nsLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return projc.KubeClient.CoreV1().Namespaces().List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return projc.KubeClient.CoreV1().Namespaces().Watch(options)
		},
	}
	go kcache.NewReflector(nsLW, &corev1.Namespace{}, projStore, 0).Run(projc.stopChan)

	projc.stopChan = stopChan
}

type SvcCache struct {
	KubeClient  kclient.Interface
	Config      *restclient.Config
	RESTMapper  apimeta.RESTMapper
	stopChan    <-chan struct{}
}

func NewSvcCache(kubeClient kclient.Interface, config *restclient.Config, mapper apimeta.RESTMapper) *SvcCache {
	return &SvcCache{
		KubeClient:  kubeClient,
		Config:      config,
		RESTMapper:  mapper,
	}
}

func (svcc *SvcCache) Run(stopChan <-chan struct{}) {
	svcc.stopChan = stopChan
	svcStore := kcache.NewStore(kcache.MetaNamespaceKeyFunc)
	svcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return svcc.KubeClient.CoreV1().Services(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return svcc.KubeClient.CoreV1().Services(metav1.NamespaceAll).Watch(options)
		},
	}
	go kcache.NewReflector(svcLW, &corev1.Service{}, svcStore, 0).Run(svcc.stopChan)
}

type ScalablesCache struct {
	ScalablesStore kcache.Store
	KubeClient  kclient.Interface
	Config      *restclient.Config
	RESTMapper  apimeta.RESTMapper
	stopChan    <-chan struct{}
}

func NewScalablesCache(kubeClient kclient.Interface, config *restclient.Config, mapper apimeta.RESTMapper) *ScalablesCache {
	return &ScalablesCache{
		ScalablesStore: kcache.NewStore(kcache.MetaNamespaceKeyFunc),
		KubeClient:  kubeClient,
		Config:      config,
		RESTMapper:  mapper,
	}
}

func (scalablesc *ScalablesCache) Run(stopChan <-chan struct{}) {
	scalablesc.stopChan = stopChan
	scalablesc.ScalablesListWatchReflect()
}

func (c *ScalablesCache) ScalablesListWatchReflect() {
	rcLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.CoreV1().ReplicationControllers(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.CoreV1().ReplicationControllers(metav1.NamespaceAll).Watch(options)
		},
	}
	rcr := kcache.NewReflector(rcLW, &corev1.ReplicationController{}, c.ScalablesStore, 0)
	go rcr.Run(c.stopChan)

	rsLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.ExtensionsV1beta1().ReplicaSets(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.ExtensionsV1beta1().ReplicaSets(metav1.NamespaceAll).Watch(options)
		},
	}
	rsr := kcache.NewReflector(rsLW, &v1beta1.ReplicaSet{}, c.ScalablesStore, 0)
	go rsr.Run(c.stopChan)

		ssLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return c.KubeClient.AppsV1beta1().StatefulSets(metav1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return c.KubeClient.AppsV1beta1().StatefulSets(metav1.NamespaceAll).Watch(options)
		},
	}
	ssr := kcache.NewReflector(ssLW, &appsv1beta1.StatefulSet{}, c.ScalablesStore, 0)
	go ssr.Run(c.stopChan)
}

// Takes a list of Pods and looks at their parent controllers
// Then takes that list of parent controllers and checks if there is another parent above them
// ex. pod -> RC -> DC, DC is the main parent controller we want to idle
func (c *ScalablesCache) FindScalableResourcesForService(pods map[string]runtime.Object) (map[corev1.ObjectReference]struct{}, error) {
	immediateControllerRefs := make(map[corev1.ObjectReference]struct{})
	for _, pod := range pods {
		controllerRef, err := GetControllerRef(pod)
		if err != nil {
			return nil, err
		}
		immediateControllerRefs[*controllerRef] = struct{}{}
	}

	controllerRefs := make(map[corev1.ObjectReference]struct{})
	for controllerRef := range immediateControllerRefs {
		controller, err := GetController(controllerRef, c.RESTMapper, c.Config)
		if err != nil {
			return nil, err
		}

		if controller != nil {
			var parentControllerRef *corev1.ObjectReference
			parentControllerRef, err = GetControllerRef(controller)
			if err != nil {
				return nil, fmt.Errorf("unable to load the creator of %s %q: %v", controllerRef.Kind, controllerRef.Name, err)
			}

			if parentControllerRef == nil {
				controllerRefs[controllerRef] = struct{}{}
			} else {
				controllerRefs[*parentControllerRef] = struct{}{}
			}
		}
	}
	return controllerRefs, nil
}

// Returns an ObjectReference to the parent controller (RC/DC/RS/Deployment) for a resource
func GetControllerRef(obj runtime.Object) (*corev1.ObjectReference, error) {
	objMeta, err := meta.Accessor(obj)
	if err != nil {
		return nil, err
	}
	ownerRef := objMeta.GetOwnerReferences()
	var ref metav1.OwnerReference
	if len(ownerRef) != 0 {
		ref = ownerRef[0]
		return &corev1.ObjectReference{
			Name:      ref.Name,
			Namespace: objMeta.GetNamespace(),
			Kind:      ref.Kind,
		}, nil
	} else {
		return nil, nil
	}
}

// Returns a generic runtime.Object for a controller
func GetController(ref corev1.ObjectReference, restMapper apimeta.RESTMapper, restConfig *restclient.Config) (runtime.Object, error) {
	// copy the config
	newConfig := *restConfig
	gv, err := schema.ParseGroupVersion(ref.APIVersion)
	if err != nil {
		return nil, err
	}
	switch ref.Kind {
	case DCKind:
		gv = v1.SchemeGroupVersion
	case DepKind, RSKind:
		gv = v1beta1.SchemeGroupVersion
	}

	mapping, err := restMapper.RESTMapping(schema.GroupKind{Group: gv.Group, Kind: ref.Kind})
	if err != nil {
		return nil, err
	}
	newConfig.GroupVersion = &gv
	switch gv.Group {
	case corev1.GroupName:
		newConfig.APIPath = "/api"
	default:
		newConfig.APIPath = "/apis"
	}

	if ref.Kind == DCKind {
		oc := appsv1.NewForConfigOrDie(&newConfig)
		oclient := oc.RESTClient()
		req := oclient.Get().
			NamespaceIfScoped(ref.Namespace, mapping.Scope.Name() == apimeta.RESTScopeNameNamespace).
			Resource(mapping.Resource).
			Name(ref.Name).Do()

		result, err := req.Get()
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	if ref.Kind == DepKind || ref.Kind == RSKind {
		extkc := extkclientv1beta1.NewForConfigOrDie(&newConfig)
		extkcclient := extkc.RESTClient()
		req := extkcclient.Get().
			NamespaceIfScoped(ref.Namespace, mapping.Scope.Name() == apimeta.RESTScopeNameNamespace).
			Resource(mapping.Resource).
			Name(ref.Name).Do()

		result, err := req.Get()
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	kc := kclientv1.NewForConfigOrDie(&newConfig)
	client := kc.RESTClient()
	req := client.Get().
		NamespaceIfScoped(ref.Namespace, mapping.Scope.Name() == apimeta.RESTScopeNameNamespace).
		Resource(mapping.Resource).
		Name(ref.Name).Do()

	result, err := req.Get()
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (c *ProjectCache) RemoveProjectsFromCacheWithoutHibernationLabel() {
	allprojs := c.Store.List()
	for p, range allprojs {
		if p.Labels == nil {
			c.store.Delete(p)
		} else {
			if p.Annotations[
			// FINISH THAT ^^
func (c *PodCache) GetProject(namespace string) (*ResourceObject, error) {
	projObj, err := c.Indexer.ByIndex("getProject", namespace)
	if err != nil {
		return nil, fmt.Errorf("couldn't get project resources: %v", err)
	}
	if e, a := 1, len(projObj); e != a {
		return nil, fmt.Errorf("expected %d project named %s, got %d", e, namespace, a)
	}

	project := projObj[0].(*ResourceObject)
	return project, nil
}

func (c *PodCache) GetProjectPods(namespace string) ([]interface{}, error) {
	namespaceAndKind := namespace + "/" + PodKind
	pods, err := c.Indexer.ByIndex("byNamespaceAndKind", namespaceAndKind)
	if err != nil {
		return nil, err
	}
	return pods, nil
}
