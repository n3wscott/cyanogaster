/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package broker

import (
	"context"
	"encoding/json"
	"fmt"

	"go.uber.org/zap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	clientset "knative.dev/eventing/pkg/client/clientset/versioned"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	servingv1 "knative.dev/serving/pkg/client/clientset/versioned/typed/serving/v1"

	"tableflip.dev/cyanogaster/pkg/reconciler/broker/resources"
)

// HACK HACK HACK
// this is going to manage custom a custom condition set for this broker.

var brokerCondSet = apis.NewLivingConditionSet(
	eventingv1.BrokerConditionAddressable,
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
	triggerLister eventinglisters.TriggerLister
	servingClient servingv1.ServingV1Interface

	image string

	// Dynamic tracker to track KResources. In particular, it tracks the dependency between Triggers and Sources.
	kresourceTracker duck.ListableTracker

	// Dynamic tracker to track AddressableTypes. In particular, it tracks DLX sinks.
	addressableTracker duck.ListableTracker
	uriResolver        *resolver.URIResolver

	// If specified, only reconcile brokers with these labels
	brokerClass string
}

// Check that our Reconciler implements Interface
var _ brokerreconciler.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, o *eventingv1.Broker) pkgreconciler.Event {
	logging.FromContext(ctx).Infow("Reconciling", zap.Any("Broker", o))

	brokerInitializeConditions(&o.Status)
	o.Status.ObservedGeneration = o.Generation

	name := resources.GenerateServiceName(o)
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

	args := &resources.Args{
		Image:  r.image,
		Broker: o,
		Labels: resources.GetLabels(),
	}

	triggers, err := r.triggerLister.List(labels.Everything())
	if err != nil {
		return err
	}
	for _, trigger := range triggers {
		if trigger.Spec.Broker == o.Name {
			if trigger.Status.SubscriberURI != nil {
				var af eventingv1.TriggerFilterAttributes
				if trigger.Spec.Filter != nil && trigger.Spec.Filter.Attributes != nil {
					af = trigger.Spec.Filter.Attributes
				}
				args.AddTrigger(resources.Trigger{
					AttributesFilter: af,
					Subscriber:       trigger.Status.SubscriberURI,
				})
			}
		}
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
		logging.FromContext(ctx).Info("Service was out of date.")
		existing.Spec = desired.Spec
		ksvc, err = r.servingClient.Services(o.Namespace).Update(ctx, existing, metav1.UpdateOptions{})
		if err != nil {
			logging.FromContext(ctx).Errorw("Failed to update broker service", zap.Any("service", existing), zap.Error(err))
			return err
		}
	}

	if ksvc.Status.Address != nil && ksvc.Status.Address.URL != nil {
		brokerSetAddress(&o.Status, ksvc.Status.Address.URL)
	} else {
		brokerSetAddress(&o.Status, nil)
	}

	return nil
}
