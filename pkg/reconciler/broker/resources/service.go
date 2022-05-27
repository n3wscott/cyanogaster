/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package resources

import (
	"fmt"
	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/kmeta"
	servingv1 "knative.dev/serving/pkg/apis/serving/v1"
	"strings"
)

func GenerateServiceName(broker *eventingv1.Broker) string {
	return strings.ToLower(fmt.Sprintf("%s-glass-broker", broker.Name))
}

func GetLabels() map[string]string {
	return map[string]string{}
}

type Args struct {
	Broker *eventingv1.Broker
	Image  string
	Labels map[string]string
}

func IsOutOfDate(a, b *servingv1.Service) bool {
	at := a.Spec.ConfigurationSpec.Template
	bt := b.Spec.ConfigurationSpec.Template
	if at.Spec.Containers[0].Image != bt.Spec.Containers[0].Image {
		return true
	}
	if !cmp.Equal(at.Spec.Containers[0].Env, bt.Spec.Containers[0].Env) {
		return true
	}

	return !cmp.Equal(at.ObjectMeta.Labels, bt.ObjectMeta.Labels)
}

func makePodSpec(args *Args) corev1.PodSpec {
	podSpec := corev1.PodSpec{
		ServiceAccountName: GenerateServiceName(args.Broker),
		Containers: []corev1.Container{{
			Image: args.Image,
			Env: []corev1.EnvVar{{
				Name:  "BROKER_NAME",
				Value: args.Broker.Name,
			}, {
				Name:  "SYSTEM_NAMESPACE",
				Value: args.Broker.Namespace,
			}, {
				Name:  "KUBERNETES_MIN_VERSION",
				Value: "v1.21.0",
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
