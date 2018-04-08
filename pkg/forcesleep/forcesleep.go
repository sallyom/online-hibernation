package forcesleep

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"github.com/openshift/online-hibernation/pkg/cache"
	"github.com/openshift/online-hibernation/pkg/idling"
	svcidler "github.com/openshift/service-idler/pkg/apis/idling/v1alpha2"
	iclient "github.com/openshift/service-idler/pkg/client/clientset/versioned/typed/idling/v1alpha2"

	"github.com/golang/glog"

	corev1 "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientv1 "k8s.io/client-go/kubernetes/typed/core/v1"
)

const (
	ComputeQuotaName          = "compute-resources"
	ComputeTimeboundQuotaName = "compute-resources-timebound"
	ProjectSleepQuotaName     = "force-sleep"
	LastSleepTimeAnnotation = "openshift.io/last-sleep-time"
	IsAsleepAnnotation = "openshift.io/is-asleep"
	ProjectSortIndexAnnotation = "openshift.io/project-sort-index"
	timeform = "2006-01-02 15:04:05.999999999 -0700 MST"
)

type SleeperConfig struct {
	Quota              time.Duration
	Period             time.Duration
	SleepSyncPeriod    time.Duration
	ProjectSleepPeriod time.Duration
	SyncWorkers        int
	Exclude            map[string]bool
	TermQuota          resource.Quantity
	NonTermQuota       resource.Quantity
	DryRun             bool
	IdlersClient	   iclient.IdlersGetter
	QuotaClient        clientv1.ResourceQuotasGetter
}

type Sleeper struct {
	config            *SleeperConfig
	podc	          *cache.PodCache
	projectc		  *cache.ProjectCache
	projectSleepQuota *corev1.ResourceQuota
	stopChan          <-chan struct{}
}

func NewSleeper(sc *SleeperConfig, podc *cache.PodCache, projc *cache.ProjectCache) *Sleeper {
	ctrl := &Sleeper{
		config:    sc,
		podc: podc,
		projectc: projc,
		projectSleepQuota: &corev1.ResourceQuota{
			ObjectMeta: metav1.ObjectMeta{
				Name: ProjectSleepQuotaName,
			},
			Spec: corev1.ResourceQuotaSpec{
				Hard: corev1.ResourceList{
					"pods": *resource.NewMilliQuantity(0, resource.DecimalSI),
				},
			},
		},
	}
	return ctrl
}

// Main function for controller
func (s *Sleeper) Run(stopChan <-chan struct{}) {
	s.stopChan = stopChan

	// Spawn a goroutine to run project sync
	go wait.Until(s.Sync, s.config.SleepSyncPeriod, stopChan)

}

// Spawns goroutines to sync projects
func (s *Sleeper) Sync() {
	glog.V(1).Infof("Force-sleeper: Running project sync")
	projects, err := s.projectc.KubeClient.CoreV1().Namespaces().List(metav1.ListOptions{})
	if err != nil {
		glog.Errorf("Force-sleeper: %s", err)
	}

    sort.Sort(Projects(projects))

	namespaces := make(chan string, len(projects.Items))
	for i := 1; i <= s.config.SyncWorkers; i++ {
		go s.startWorker(namespaces)
	}
	for _, namespace := range projects.Items {
		namespaces <- namespace.ObjectMeta.Name
	}
	close(namespaces)
}

func (s *Sleeper) startWorker(namespaces <-chan string) {
	for namespace := range namespaces {
		s.syncProject(namespace)
	}
}

func (s *Sleeper) applyProjectSleep(namespace string, sleepTime, wakeTime time.Time) error {
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Simulating project( %s )sleep in dry run mode", namespace)
	}
	//TODO: Convert to kube GetProject
	proj, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	projectCopy := proj.DeepCopy()
	projectCopy.Annotations[LastSleepTimeAnnotation] = sleepTime.String()
	projectCopy.Annotations[IsAsleepAnnotation] = "true"
	_, err = s.projectc.KubeClient.CoreV1().Namespaces().Update(projectCopy)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	// Check that resource was updated
	sleepingProj, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	// Fix this, also have to check if annot exists with ok, &&
	if sleepingProj.Annotations[IsAsleepAnnotation] != "true" {
		return fmt.Errorf("Force-sleeper: Sleeping project( %s )is missing '%s=true' label.", namespace)
	}
	// Do not make any quota, idling, or scaling changes in dry-run mode
	if s.config.DryRun {
		return nil
	}
	// Add force-sleep annotation to project
	err = s.addNamespaceSleepTimeAnnotation(namespace, sleepTime)
	if err != nil {
		if !kerrors.IsAlreadyExists(err) {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		glog.V(2).Infof("Force-sleeper: Couldn't add project( %s )sleep time annotation: %s", namespace, err)
	}

	// Add force-sleep resource quota to object
	_, err = s.config.QuotaClient.ResourceQuotas(namespace).Create(s.projectSleepQuota)
	if err != nil {
		return fmt.Errorf("force-sleeper: %s", err)
	}

	failed := false

	glog.V(2).Infof("Force-sleeper: Scaling Resources in project( %s ) to 0", namespace)
	// TODO: Write this
	err = idling.ScaleProjectScalables(s.projectc, namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error scaling DCs in project( %s ): %s", namespace, err)
	}

	glog.V(2).Infof("Force-sleeper: Clearing cache for project( %s )", namespace)
	err = s.clearProjectCache(namespace)
	if err != nil {
		failed = true
		glog.Errorf("Force-sleeper: Error clearing cache in project( %s ): %s", namespace, err)
	}
	if failed {
		glog.Errorf("Force-sleeper: Error applying sleep for project %s", namespace)
	}
	return nil
}

// Adds a cache.LastSleepTimeAnnotation to a namespace in the cluster
func (s *Sleeper) addNamespaceSleepTimeAnnotation(name string, sleepTime time.Time) error {
	ns, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(name, metav1.GetOptions{})
	projCopy := ns.DeepCopy()
	if projCopy.Annotations == nil {
		projCopy.Annotations = make(map[string]string)
	}
	projCopy.Annotations[cache.LastSleepTimeAnnotation] = sleepTime.String()
	_, err = s.projectc.KubeClient.CoreV1().Namespaces().Update(projCopy)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	return nil
}

// Removes the cache.LastSleepTimeAnnotation to a namespace in the cluster
func (s *Sleeper) removeNamespaceSleepTimeAnnotation(name string) error {
	ns, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(name, metav1.GetOptions{})
	projCopy := ns.DeepCopy()
	if projCopy.Annotations == nil {
		// Sleep time annotation already doesn't exist
		return nil
	}

	delete(projCopy.Annotations, cache.LastSleepTimeAnnotation)
	_, err = s.projectc.KubeClient.CoreV1().Namespaces().Update(projCopy)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	return nil
}

// clearProjectCache removes all pods from ProjPodCache for given namespace
func (s *Sleeper) clearProjectCache(namespace string) error {
	pods, err := s.podc.Indexer.ByIndex("byNamespace", namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	for _, pod := range pods {
		rpod := pod.(*cache.ResourceObject)
		if rpod.Kind == "pod" {
			s.podc.Indexer.DeleteResourceObject(rpod)
		}
	}
	return nil
}

// wakeProject checks if a project needs to wake up and if so, takes the actions to do that
func (s *Sleeper) wakeProject(project *corev1.Namespace) error {
	namespace := project.Namespace
	if s.config.DryRun {
		glog.V(2).Infof("Force-sleeper DryRun: Simulating project( %s )wake in dry run mode", namespace)
	}
	failed := false
	lastSleepTime, err := time.Parse(timeform, project.ObjectMeta.Annotations[LastSleepTimeAnnotation])
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	if time.Since(lastSleepTime) > s.config.ProjectSleepPeriod {
		// First remove the force-sleep pod count resource quota from the project
		glog.V(2).Infof("Force-sleeper: Removing sleep quota for project( %s )", namespace)
		if !s.config.DryRun {
			quotaInterface := s.config.QuotaClient.ResourceQuotas(namespace)
			err := quotaInterface.Delete(ProjectSleepQuotaName, &metav1.DeleteOptions{})
			if err != nil {
				if !kerrors.IsNotFound(err) {
					return fmt.Errorf("Force-sleeper: Error removing sleep quota in project( %s ): %s", namespace, err)

				}
				glog.V(2).Infof("Force-sleeper: Sleep quota not found in project( %s ), could not remove", namespace)
			}
			// Remove sleep time annotation, if it exists
			err = s.removeNamespaceSleepTimeAnnotation(namespace)
			if err != nil {
				failed = true
				glog.Errorf("Force-sleeper: Error removing project( %s )sleep time annotation: %s", namespace, err)
			}
			glog.V(2).Infof("Force-sleeper: Idling services in project( %s )", namespace)
			err = idling.IdleProject(namespace)
			if err != nil {
				glog.V(2).Infof("Force-sleeper: Error idling services in project( %s )", namespace)
			}
		} else {
			err := s.SetDryRunResourceRuntimes(namespace)
			if err != nil {
				return fmt.Errorf("Force-sleeper DryRun: Error simulating force-sleep wakeup in project: %v", err)
			}
		}

		projectCopy := project.DeepCopy()
		// TODO, is this right?Does time.Time{} need String()?
		projectCopy.Annotations[LastSleepTimeAnnotation] = time.Time{}.String()
		projectCopy.Annotations[IsAsleepAnnotation] = "false"
		_, err = s.projectc.KubeClient.CoreV1().Namespaces().Update(projectCopy)
		// Check that resource was updated
		awakeProj, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		if awakeProj.Annotations[IsAsleepAnnotation] == "true" {
			return fmt.Errorf("Project( %s )is set to 'IsAsleep' but should not be.", namespace)
		}
		if failed {
			glog.Errorf("Force-sleeper: Failed to wake all services in project( %s )", namespace)
		}
		return nil
	}
	return nil
}

func (s *Sleeper) memoryQuota(pod *cache.ResourceObject) resource.Quantity {
	// Get project memory quota
	if pod.Terminating {
		return s.config.TermQuota
	} else {
		return s.config.NonTermQuota
	}

}

// Modify all pods' cached runtimes when removing force-sleep
// when running with Dry Run mode enabled, to better simulate real run times
// (since in Dry Run mode, no resources are actually scaled or deleted with force-sleep)
func (s *Sleeper) SetDryRunResourceRuntimes(namespace string) error {
	glog.V(2).Infof("Force-sleeper DryRun: Simulating resource runtimes for namespace( %s ).", namespace)
	pods, err := s.podc.Indexer.ByIndex("byNamespace", namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	for _, pod := range pods {
		p := pod.(*cache.ResourceObject)
		podCopy := p.DeepCopy()
		// If a resource is currently running, remove all its previous runtimes (simulating
		// applied force-sleep) and set its most recent start time to now (simulating wakeup)
		// Otherwise, just delete all previous start times
		newRunningTimes := make([]*cache.RunningTime, 0)
		if podCopy.IsStarted() {
			newRunningTime := &cache.RunningTime{
				Start: time.Now(),
			}
			newRunningTimes = append(newRunningTimes, newRunningTime)
		}
		podCopy.RunningTimes = newRunningTimes
		s.podc.Indexer.UpdateResourceObject(podCopy)
	}
	return nil
}

// Syncs a project and determines if force-sleep is needed
func (s *Sleeper) syncProject(namespace string) error {
	idlerInterface := s.config.IdlersClient.Idlers(namespace)
	glog.V(2).Infof("Force-sleeper: Syncing project( %s )", namespace)
	project, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}

	// Iterate through pods to calculate runtimes
	pods, err := s.podc.GetProjectPods(namespace)
	if err != nil {
		return fmt.Errorf("Force-sleeper: %s", err)
	}
	termQuotaSecondsConsumed := 0.0
	nonTermQuotaSecondsConsumed := 0.0
	for _, obj := range pods {
		pod := obj.(*cache.ResourceObject)
		if s.PrunePods(pod) {
			continue
		}
		newPod := pod.DeepCopy()
		totalRuntime, changed := newPod.GetResourceRuntime(s.config.Period)
		if changed {
			s.podc.Indexer.UpdateResourceObject(newPod)
		}
		seconds := float64(totalRuntime.Seconds())
		memoryLimit := s.memoryQuota(pod)
		quotaSeconds := getQuotaSeconds(seconds, pod.MemoryRequested, memoryLimit)
		if pod.Terminating {
			termQuotaSecondsConsumed += quotaSeconds
		} else {
			nonTermQuotaSecondsConsumed += quotaSeconds
		}
	}
	quotaSecondsConsumed := math.Max(termQuotaSecondsConsumed, nonTermQuotaSecondsConsumed)

	//Check if quota doesn't exist and should
	// TODO: Fix, also check if annotation exists
	if project.ObjectMeta.Annotations != nil {
		if project.ObjectMeta.Annotations[LastSleepTimeAnnotation] != "" {
		lastSleepTime, err := time.Parse(timeform, namespace.ObjectMeta.Annotations[LastSleepTimeAnnotation])
		if err != nil {
			return fmt.Errorf("Force-sleeper: %s", err)
		}
		if time.Since(lastSleepTime) < s.config.ProjectSleepPeriod {
			err = s.applyProjectSleep(namespace, lastSleepTime, lastSleepTime.Add(s.config.ProjectSleepPeriod))
			if err != nil {
				return fmt.Errorf("Force-sleeper: %s", err)
			}
			return nil
		} else {
			// Wake the project if project has been asleep longer than force-sleep period
			err := s.wakeProject(project)
			if err != nil {
				return fmt.Errorf("Force-sleeper: %s", err)
			}
			return nil
		}
	}
}
	if quotaSecondsConsumed > s.config.Quota.Seconds() {
		// Project-level sleep
		glog.V(2).Infof("Force-sleeper: Project( %s )over quota! (%+vs/%+vs), applying force-sleep...", namespace, quotaSecondsConsumed, s.config.Quota.Seconds())
		err = s.applyProjectSleep(namespace, time.Now(), time.Now().Add(s.config.ProjectSleepPeriod))
		if err != nil {
			return fmt.Errorf("Force-sleeper: Error applying project( %s )sleep quota: %s", namespace, err)
		}
		return nil
	}

	// Project sort index:
	s.updateProjectSortIndex(namespace, quotaSecondsConsumed)
	glog.V(2).Infof("Force-sleeper: Project( %s )sync complete", namespace)
	return nil
}

func (s *Sleeper) updateProjectSortIndex(namespace string, quotaSeconds float64) {
	proj, err := s.projectc.KubeClient.CoreV1().Namespaces().Get(namespace, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("Force-sleeper: Error getting project resources: %s", err)
	}
	projectCopy := proj.DeepCopy()

	// Projects closer to force-sleep will have a lower index value
	sortIndex := -1 * quotaSeconds
	strsortIndex := strconv.FormatFloat(sortIndex, 'f', 6, 64)
	projectCopy.Annotations[ProjectSortIndexAnnotation] = strsortIndex
	_, err = s.projectc.KubeClient.CoreV1().Namespaces().Update(projectCopy)
	if err != nil {
		glog.Errorf("Force-sleeper: %s", err)
	}
}

// Check to clear cached resources whose runtimes are outside of the period, and thus irrelevant
func (s *Sleeper) PrunePods(resource *cache.ResourceObject) bool {
	count := len(resource.RunningTimes)
	if count < 1 {
		return true
	}
	if resource.IsStarted() {
		return false
	}
	lastTime := resource.RunningTimes[count-1]
	if time.Since(lastTime.End) > s.config.Period {
		switch resource.Kind {
		case cache.PodKind:
			s.podc.Indexer.DeleteResourceObject(resource)
			return true
		default:
			glog.Errorf("Object passed to Prune Resource Not a Pod")
		}
	}
	return false
}
