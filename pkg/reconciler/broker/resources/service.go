/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package resources

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	v1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/kmeta"
	"knative.dev/pkg/logging"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"sort"
	"strings"
)

func GenerateServiceName(broker *eventingv1.Broker) string {
	return strings.ToLower(fmt.Sprintf("%s-glass-broker", broker.Name))
}

func GetLabels() map[string]string {
	return map[string]string{
		"todo": "true",
	}
}

type Args struct {
	Broker   *eventingv1.Broker
	Image    string
	Labels   map[string]string
	Triggers []Trigger
}

func (a *Args) AddTrigger(t Trigger) {
	logging.FromContext(context.Background()).Info(a.Broker.Name, "adding trigger", t.Subscriber.String())
	if a.Triggers == nil {
		a.Triggers = make([]Trigger, 0, 1)
	}
	a.Triggers = append(a.Triggers, t)
}

type Trigger struct {
	AttributesFilter eventingv1.TriggerFilterAttributes `json:"af,omitempty"`
	Subscriber       *apis.URL                          `json:"s,omitempty"`
}

type Delivery struct {
	Spec   *v1.DeliverySpec   `json:"sp,omitempty"`
	Status *v1.DeliveryStatus `json:"st,omitempty"`
}

func triggersEqual(a, b string) bool {
	var da []Trigger
	if err := json.Unmarshal([]byte(a), &da); err != nil {
		return false
	}
	sort.SliceStable(da, func(i, j int) bool {
		return strings.Compare(da[i].Subscriber.String(), da[j].Subscriber.String()) == -1
	})
	var db []Trigger
	if err := json.Unmarshal([]byte(b), &db); err != nil {
		return false
	}
	sort.SliceStable(db, func(i, j int) bool {
		return strings.Compare(db[i].Subscriber.String(), db[j].Subscriber.String()) == -1
	})
	return cmp.Equal(&da, &db)
}

func deliveryEqual(a, b string) bool {
	var da Delivery
	if err := json.Unmarshal([]byte(a), &da); err != nil {
		return false
	}
	var db Delivery
	if err := json.Unmarshal([]byte(b), &db); err != nil {
		return false
	}
	return cmp.Equal(&da, &db)
}

func IsOutOfDate(a, b *servingv1.Service) bool {
	at := a.Spec.ConfigurationSpec.Template
	bt := b.Spec.ConfigurationSpec.Template
	if at.Spec.Containers[0].Image != bt.Spec.Containers[0].Image {
		return true
	}

	{
		ta := ""
		for _, e := range at.Spec.Containers[0].Env {
			if e.Name == "TRIGGERS" {
				ta = e.Value
			}
		}
		tb := ""
		for _, e := range bt.Spec.Containers[0].Env {
			if e.Name == "TRIGGERS" {
				tb = e.Value
			}
		}
		if !triggersEqual(ta, tb) {
			return true
		}
	}
	{
		da := ""
		for _, e := range at.Spec.Containers[0].Env {
			if e.Name == "DELIVERY" {
				da = e.Value
			}
		}
		db := ""
		for _, e := range bt.Spec.Containers[0].Env {
			if e.Name == "DELIVERY" {
				db = e.Value
			}
		}
		if !deliveryEqual(da, db) {
			return true
		}
	}

	return !cmp.Equal(at.ObjectMeta.Labels, bt.ObjectMeta.Labels)
}

func makePodSpec(args *Args) corev1.PodSpec {
	delivery := Delivery{
		Spec:   args.Broker.Spec.Delivery,
		Status: &args.Broker.Status.DeliveryStatus,
	}
	deliveryJson, _ := json.Marshal(delivery)
	if deliveryJson == nil || len(deliveryJson) == 0 || string(deliveryJson) == "null" {
		deliveryJson = []byte("{}")
	}

	sort.SliceStable(args.Triggers, func(i, j int) bool {
		return strings.Compare(args.Triggers[i].Subscriber.String(), args.Triggers[j].Subscriber.String()) == -1
	})

	triggerJson, _ := json.Marshal(args.Triggers)
	if triggerJson == nil || len(triggerJson) == 0 || string(triggerJson) == "null" {
		triggerJson = []byte("[]")
	}

	podSpec := corev1.PodSpec{
		Containers: []corev1.Container{{
			Image: args.Image,
			Env: []corev1.EnvVar{{
				Name:  "TRIGGERS",
				Value: string(triggerJson),
			}, {
				Name:  "DELIVERY",
				Value: string(deliveryJson),
			}},
		}},
	}
	return podSpec
}

func MakeService(args *Args) *servingv1.Service {
	podSpec := makePodSpec(args)

	return &servingv1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       args.Broker.Namespace,
			Name:            GenerateServiceName(args.Broker),
			Labels:          args.Labels,
			OwnerReferences: []metav1.OwnerReference{*kmeta.NewControllerRef(args.Broker)},
		},
		Spec: servingv1.ServiceSpec{
			ConfigurationSpec: servingv1.ConfigurationSpec{
				Template: servingv1.RevisionTemplateSpec{
					ObjectMeta: metav1.ObjectMeta{
						Labels: args.Labels,
						// DEBUGGING
						Annotations: map[string]string{
							"autoscaling.knative.dev/minScale": "1",
							"autoscaling.knative.dev/maxScale": "1",
						},
					},
					Spec: servingv1.RevisionSpec{
						PodSpec: podSpec,
					},
				},
			},
		},
	}
}
