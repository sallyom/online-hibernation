package cache

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	fakekclientset "k8s.io/client-go/kubernetes/fake"

	ktesting "k8s.io/client-go/testing"
	kcache "k8s.io/client-go/tools/cache"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

func setupClients(t *testing.T) (*fakekclientset.Clientset, time.Time) {
	maxDeployDurSeconds := int64(8)
	deltimenow := time.Now()
	deltimeunver := metav1.NewTime(deltimenow)
	deltime := &deltimeunver
	fakeClient := &fakekclientset.Clientset{}

	fakeClient.AddReactor("list", "namespaces", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &corev1.NamespaceList{
			Items: []corev1.Namespace{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:   "somens1",
						Labels: map[string]string{"openshift.io/hibernate-include": "true"},
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Labels: map[string]string{"openshift.io/hibernate-include": "true"},
						Name:   "somens2",
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceTerminating,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:   "somens3",
						Labels: map[string]string{"openshift.io/hibernate-include": "true"},
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name: "somens4",
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Namespace",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:   "somens5",
						Labels: map[string]string{"openshift.io/hibernate-include": "true"},
					},
					Status: corev1.NamespaceStatus{
						Phase: corev1.NamespaceActive,
					},
				},
			},
		}

		return true, obj, nil
	})


	fakeClient.AddReactor("list", "pods", func(action ktesting.Action) (handled bool, resp runtime.Object, err error) {
		obj := &corev1.PodList{
			Items: []corev1.Pod{
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod1",
						Namespace:         "somens1",
						UID:               "1122",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"somepodlabel": "podsomething"},
						Annotations:       map[string]string{OpenShiftDCName: "poddc"},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyOnFailure,
						ActiveDeadlineSeconds: &maxDeployDurSeconds,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod2",
						Namespace:         "somens5",
						UID:               "2211",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"anotherpodlabel": "podsomethingelse"},
						Annotations:       map[string]string{OpenShiftDCName: "anotherpoddc"},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodUnknown,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod3",
						Namespace:         "somens3",
						UID:               "2211",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"yetanotherpodlabel": "podsomethingelseagain"},
						Annotations:       map[string]string{},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
				{
					TypeMeta: metav1.TypeMeta{
						Kind:       "Pod",
						APIVersion: "v1",
					},
					ObjectMeta: metav1.ObjectMeta{
						Name:              "somepod4",
						Namespace:         "somens3",
						UID:               "2222",
						DeletionTimestamp: deltime,
						Labels:            map[string]string{"yetanotherpodlabelagain": "podsomethingelseagainandagain"},
						Annotations:       map[string]string{},
					},
					Spec: corev1.PodSpec{
						RestartPolicy:         corev1.RestartPolicyAlways,
						ActiveDeadlineSeconds: nil,
						Containers: []corev1.Container{
							{
								Resources: corev1.ResourceRequirements{
									Requests: corev1.ResourceList{
										"memory": resource.MustParse("0"),
									},
								},
							},
						},
					},
					Status: corev1.PodStatus{
						Phase: corev1.PodRunning,
					},
				},
			},
		}

		return true, obj, nil
	})

	return fakeClient, deltimenow
}

func TestPodCacheIsInitiallyPopulated(t *testing.T) {
	c := make(chan struct{})
	fakeClient, deltime := setupClients(t)
	podstore := NewResourceStore(fakeClient)
	projstore := kcache.NewStore(kcache.MetaNamespaceKeyFunc)

	podLW := &kcache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return fakeClient.CoreV1().Pods(corev1.NamespaceAll).List(options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return fakeClient.CoreV1().Pods(corev1.NamespaceAll).Watch(options)
		},
	}

	podr := kcache.NewReflector(podLW, &corev1.Pod{}, podstore, 0)
	go podr.Run(c)

	projLW := &kcache.ListWatch{
		ListFunc: func(opts metav1.ListOptions) (runtime.Object, error) {
			selectormap := map[string]string{HibernationLabel: "true"}
			selector, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: selectormap})
			opts.LabelSelector = selector.String()
			return fakeClient.CoreV1().Namespaces().List(opts)
		},
		WatchFunc: func(opts metav1.ListOptions) (watch.Interface, error) {
			selectormap := map[string]string{HibernationLabel: "true"}
			selector, _ := metav1.LabelSelectorAsSelector(&metav1.LabelSelector{MatchLabels: selectormap})
			opts.LabelSelector = selector.String()
			return fakeClient.CoreV1().Namespaces().Watch(opts)
		},
	}
	go kcache.NewReflector(projLW, &corev1.Namespace{}, projstore, 0).Run(c)

	time.Sleep(1 * time.Second)

	// somens1: Active project with hibernation label, 1 running pod
	_, exists, err := projstore.GetByKey("somens1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, exists, true, "expected 1 project 'somens1' in cache")

	// somepod1 should be converted to ResourceObject properly
	pods, err := podstore.ByIndex("byNamespaceAndKind", "somens1/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if assert.Len(t, pods, 1, "expected to have 1 running pod in cache in namespace 'somens1'") {
		resource := &ResourceObject{
			UID:               "1122",
			Name:              "somepod1",
			Namespace:         "somens1",
			Kind:              PodKind,
			Terminating:       true,
			DeletionTimestamp: deltime,
			Labels:            map[string]string{"somepodlabel": "podsomething"},
			Annotations:       map[string]string{OpenShiftDCName: "poddc"},
		}
		assert.NotNil(t, pods[0].(*ResourceObject).RunningTimes, "expected the pod somepod1 to be converted to a resource object properly, found nil RunningTime")
		assert.NotNil(t, pods[0].(*ResourceObject).MemoryRequested, "expected the pod somepod1 to be converted to a resource object properly, found nil RunningTime")
		assert.Equal(t, resource.UID, pods[0].(*ResourceObject).UID, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Name, pods[0].(*ResourceObject).Name, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Namespace, pods[0].(*ResourceObject).Namespace, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Kind, pods[0].(*ResourceObject).Kind, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Annotations, pods[0].(*ResourceObject).Annotations, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.DeletionTimestamp, pods[0].(*ResourceObject).DeletionTimestamp, "expected the pod somepod1 to be converted to a resource object properly")
		assert.Equal(t, resource.Labels, pods[0].(*ResourceObject).Labels, "expected the pod somepod1 to be converted to a resource object properly")
	}

	// somens3 should have 2 running pods 
	pods, err = podstore.ByIndex("byNamespaceAndKind", "somens3/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Len(t, pods, 2, "expected to have 2 running pods in namespace somens3")

	// somens2 is terminating, it's pods should not be added to cache
	pods, err = podstore.ByIndex("byNamespaceAndKind", "somens2/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Len(t, pods, 0, "expected to have 0 running pods in namespace somens2")

	// somens5 is active, should be added to cache
	_, exists, err = projstore.GetByKey("somens5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, exists, true, "expected 1 project 'somens5' in cache")

	// pods in somens5 is not Phase PodRunning, so it should not be added to cache
	pods, err = podstore.ByIndex("byNamespaceAndKind", "somens5/"+PodKind)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Len(t, pods, 0, "expected to have 0 running pods in namespace somens5, bc pod Phase:Unknown")

	// Expect somens4 to be excluded bc it does not have the OnlineHibernation label
	_, exists, err = projstore.GetByKey("somens4")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	assert.Equal(t, exists, false, "expected no project 'somens4', because somens4 has no openshift.io/hibernate-include label")
}
