/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/google/go-cmp/cmp"
	"k8s.io/client-go/kubernetes"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	clientset "knative.dev/eventing/pkg/client/clientset/versioned"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	servingv1 "knative.dev/serving/pkg/client/clientset/versioned/typed/serving/v1"

	"tableflip.dev/cyanogaster/pkg/reconciler/broker/resources"
)

// HACK HACK HACK
// this is going to manage custom a custom condition set for this broker.
const (
	ConditionService apis.ConditionType = "Service"
)

var brokerCondSet = apis.NewLivingConditionSet(
	eventingv1.BrokerConditionAddressable,
	ConditionService,
)

// InitializeConditions sets relevant unset conditions to Unknown state.
func brokerInitializeConditions(bs *eventingv1.BrokerStatus) {
	bs.Status.Conditions = nil
	brokerCondSet.Manage(bs).InitializeConditions()
}

// SetAddress makes this Broker addressable by setting the hostname. It also
// sets the BrokerConditionAddressable to true.
func brokerSetAddress(bs *eventingv1.BrokerStatus, url *apis.URL) {
	if url != nil {
		bs.Address.URL = url
		brokerCondSet.Manage(bs).MarkTrue(eventingv1.BrokerConditionAddressable)
	} else {
		bs.Address.URL = nil
		brokerCondSet.Manage(bs).MarkFalse(eventingv1.BrokerConditionAddressable, "NotAddressable", "broker service has .status.addressable.url == nil")
	}
}

type Reconciler struct {
	eventingClientSet clientset.Interface

	// listers index properties about resources
	brokerLister  eventinglisters.BrokerLister
	servingClient servingv1.ServingV1Interface

	kubeClient kubernetes.Interface

	image string

	uriResolver *resolver.URIResolver
}

// Check that our Reconciler implements Interface
var _ brokerreconciler.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, o *eventingv1.Broker) pkgreconciler.Event {
	logging.FromContext(ctx).Infow("Reconciling", zap.Any("Broker", o))

	brokerInitializeConditions(&o.Status)

	if deadLetterEnabled(o) {
		dlqURI, err := r.uriResolver.URIFromDestinationV1(ctx, *o.Spec.Delivery.DeadLetterSink, o)
		if err != nil {
			logging.FromContext(ctx).Errorw("Unable to get the DeadLetterSink's URI", zap.Error(err))
			// TODO: mark status of dld
			o.Status.DeadLetterSinkURI = nil
			return err
		}
		o.Status.DeadLetterSinkURI = dlqURI
	}

	name := resources.GenerateServiceName(o)
	args := &resources.Args{
		Image:  r.image,
		Broker: o,
	}

	// Service Account
	{
		// TODO: use the lister to fetch?
		existing, err := r.kubeClient.CoreV1().ServiceAccounts(o.Namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				logging.FromContext(ctx).Errorw("Unable to get an existing ServiceAccount", zap.Error(err))
				return err
			}
			existing = nil
		} else if !metav1.IsControlledBy(existing, o) {
			s, _ := json.Marshal(existing)
			logging.FromContext(ctx).Errorw("Broker does not own ServiceAccount", zap.Any("serviceAccount", s))
			return fmt.Errorf("Broker %q does not own ServiceAccount: %q", o.Name, name)
		}
		desired := resources.MakeServiceAccount(args)
		if existing == nil {
			_, err = r.kubeClient.CoreV1().ServiceAccounts(o.Namespace).Create(ctx, desired, metav1.CreateOptions{})
			if err != nil {
				logging.FromContext(ctx).Errorw("Failed to create broker service account", zap.Error(err))
				return err
			}
		}
	}

	// Role Binding
	{
		name := name + "-" + args.Broker.Namespace
		// TODO: use the lister to fetch?
		existing, err := r.kubeClient.RbacV1().ClusterRoleBindings().Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if !apierrors.IsNotFound(err) {
				logging.FromContext(ctx).Errorw("Unable to get an existing ClusterRoleBinding", zap.Error(err))
				return err
			}
			existing = nil
		} else if !metav1.IsControlledBy(existing, o) {
			s, _ := json.Marshal(existing)
			logging.FromContext(ctx).Errorw("Broker does not own ServiceAccount", zap.Any("serviceAccount", s))
			return fmt.Errorf("Broker %q does not own ClusterRoleBinding: %q", o.Name, name)
		}
		desired := resources.MakeBinding(args)
		if existing == nil {
			_, err = r.kubeClient.RbacV1().ClusterRoleBindings().Create(ctx, desired, metav1.CreateOptions{})
			if err != nil {
				logging.FromContext(ctx).Errorw("Failed to create ClusterRoleBinding", zap.Error(err))
				return err
			}
		}
	}

	// TODO: use the lister to fetch the service?
	existing, err := r.servingClient.Services(o.Namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if !apierrors.IsNotFound(err) {
			logging.FromContext(ctx).Errorw("Unable to get an existing Service", zap.Error(err))
			return err
		}
		existing = nil
	} else if !metav1.IsControlledBy(existing, o) {
		s, _ := json.Marshal(existing)
		logging.FromContext(ctx).Errorw("Broker does not own Service", zap.Any("service", s))
		return fmt.Errorf("Broker %q does not own Service: %q", o.Name, name)
	}

	desired := resources.MakeService(args)

	logging.FromContext(ctx).Info("Made a service, comparing it now..")

	ksvc := existing
	if existing == nil {
		ksvc, err = r.servingClient.Services(o.Namespace).Create(ctx, desired, metav1.CreateOptions{})
		if err != nil {
			logging.FromContext(ctx).Errorw("Failed to create broker service", zap.Error(err))
			return err
		}
	} else if resources.IsOutOfDate(existing, desired) {
		logging.FromContext(ctx).Info("Service was out of date.", cmp.Diff(existing, desired))
		existing.Spec = desired.Spec
		ksvc, err = r.servingClient.Services(o.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			logging.FromContext(ctx).Errorw("Failed to update broker service", zap.Any("service", existing), zap.Error(err))
			return err
		}
	}

	if kr := ksvc.Status.GetCondition(apis.ConditionReady); kr != nil {
		if kr.IsTrue() {
			brokerCondSet.Manage(&o.Status).MarkTrue(ConditionService)
		} else if kr.IsUnknown() {
			brokerCondSet.Manage(&o.Status).MarkUnknown(ConditionService, kr.Reason, kr.Message)
		} else {
			brokerCondSet.Manage(&o.Status).MarkFalse(ConditionService, kr.Reason, kr.Message)
		}
	}

	if ksvc.Status.Address != nil && ksvc.Status.Address.URL != nil {
		brokerSetAddress(&o.Status, ksvc.Status.Address.URL)
	} else {
		brokerSetAddress(&o.Status, nil)
	}

	return nil
}

func deadLetterEnabled(b *eventingv1.Broker) bool {
	return b.Spec.Delivery != nil && b.Spec.Delivery.DeadLetterSink != nil
}
