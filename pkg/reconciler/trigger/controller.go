/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package trigger

import (
	"context"
	"log"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	"knative.dev/pkg/resolver"
)

const BrokerClass = "GlassBroker"

// NewController initializes the controller and is called by the generated code
// Registers event handlers to enqueue events
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {
	triggerInformer := triggerinformer.Get(ctx)
	brokerInformer := brokerinformer.Get(ctx)

	r := &Reconciler{
		brokerLister: brokerInformer.Lister(),
		brokerClass:  BrokerClass,
	}
	impl := triggerreconciler.NewImpl(ctx, r)

	logging.FromContext(ctx).Info("Setting up event handlers")

	r.addressableTracker = duck.NewListableTrackerFromTracker(ctx, addressable.Get, impl.Tracker)
	r.uriResolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	brokerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if broker, ok := obj.(*eventingv1.Broker); ok {
				label := broker.ObjectMeta.Annotations[brokerreconciler.ClassAnnotationKey]
				if label != BrokerClass {
					return
				}
				triggers, err := triggerInformer.Lister().Triggers(broker.Namespace).List(labels.Everything())
				if err != nil {
					log.Print("Failed to lookup Triggers for Broker", zap.Error(err))
				} else {
					for _, t := range triggers {
						if t.Spec.Broker == broker.Name {
							impl.Enqueue(t)
						}
					}
				}
			}
		},
	))

	triggerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if trigger, ok := obj.(*eventingv1.Trigger); ok {
				broker, err := brokerInformer.Lister().Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
				if err != nil {
					log.Print("Failed to lookup Broker for Trigger", zap.Error(err))
				} else {
					label := broker.ObjectMeta.Annotations[brokerreconciler.ClassAnnotationKey]
					if label == BrokerClass {
						impl.Enqueue(obj)
					}
				}
			}
		},
	))

	return impl
}
