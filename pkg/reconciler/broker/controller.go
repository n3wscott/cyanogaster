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
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	kubeclient "knative.dev/pkg/client/injection/kube/client"
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
	serviceInformer := serviceinformer.Get(ctx)

	r := &Reconciler{
		image:             env.Image,
		eventingClientSet: eventingclient.Get(ctx),
		brokerLister:      brokerInformer.Lister(),
		servingClient:     client.Get(ctx).ServingV1(),
		kubeClient:        kubeclient.Get(ctx),
	}

	impl := brokerreconciler.NewImpl(ctx, r, BrokerClass)

	logging.FromContext(ctx).Info("Setting up event handlers")

	r.uriResolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: pkgreconciler.AnnotationFilterFunc(brokerreconciler.ClassAnnotationKey, BrokerClass, false /*allowUnset*/),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	serviceInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: controller.FilterController(&eventingv1.Broker{}),
		Handler:    controller.HandleAll(impl.EnqueueControllerOf),
	})

	return impl
}
