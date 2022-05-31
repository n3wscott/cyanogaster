/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package resources

import (
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/kmeta"
)

func MakeServiceAccount(args *Args) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       args.Broker.Namespace,
			Name:            GenerateServiceName(args.Broker),
			OwnerReferences: []metav1.OwnerReference{*kmeta.NewControllerRef(args.Broker)},
		},
	}
}

func MakeBinding(args *Args) *rbacv1.ClusterRoleBinding {
	return &rbacv1.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:       args.Broker.Namespace,
			Name:            GenerateServiceName(args.Broker) + "-" + args.Broker.Namespace,
			OwnerReferences: []metav1.OwnerReference{*kmeta.NewControllerRef(args.Broker)},
		},
		Subjects: []rbacv1.Subject{{
			Kind:      "ServiceAccount",
			Name:      GenerateServiceName(args.Broker),
			Namespace: args.Broker.Namespace,
		}},
		RoleRef: rbacv1.RoleRef{
			Kind:     "ClusterRole",
			APIGroup: "rbac.authorization.k8s.io",
			Name:     "glass-broker-viewer",
		},
	}
}
