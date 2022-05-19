/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package broker

import (
	"context"
	"log"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	eventingclient "knative.dev/eventing/pkg/client/injection/client"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/client/injection/ducks/duck/v1/addressable"
	"knative.dev/pkg/client/injection/ducks/duck/v1/conditions"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
	"knative.dev/serving/pkg/client/injection/client"
	serviceinformer "knative.dev/serving/pkg/client/injection/informers/serving/v1/service"
)

type envConfig struct {
	Image string `envconfig:"GLASS_BROKER_IMAGE" required:"true"`
}

const BrokerClass = "GlassBroker"

// NewController initializes the controller and is called by the generated code
// Registers event handlers to enqueue events
func NewController(
	ctx context.Context,
	cmw configmap.Watcher,
) *controller.Impl {

	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Fatal("Failed to process env var", zap.Error(err))
	}

	brokerInformer := brokerinformer.Get(ctx)
	triggerInformer := triggerinformer.Get(ctx)
	serviceInformer := serviceinformer.Get(ctx)

	r := &Reconciler{
		image:             env.Image,
		eventingClientSet: eventingclient.Get(ctx),
		brokerLister:      brokerInformer.Lister(),
		triggerLister:     triggerInformer.Lister(),
		brokerClass:       BrokerClass,
		servingClient:     client.Get(ctx).ServingV1(),
	}

	impl := brokerreconciler.NewImpl(ctx, r, BrokerClass)

	logging.FromContext(ctx).Info("Setting up event handlers")

	r.kresourceTracker = duck.NewListableTrackerFromTracker(ctx, conditions.Get, impl.Tracker)
	r.addressableTracker = duck.NewListableTrackerFromTracker(ctx, addressable.Get, impl.Tracker)
	r.uriResolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: pkgreconciler.AnnotationFilterFunc(brokerreconciler.ClassAnnotationKey, BrokerClass, false /*allowUnset*/),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterController(&eventingv1.Broker{}),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

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
