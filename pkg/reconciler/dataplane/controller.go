/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package dataplane

import (
	"context"
	"log"
	"net/http"
	"sync/atomic"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"k8s.io/client-go/tools/cache"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/system"
	pkgtracing "knative.dev/pkg/tracing"
)

type envConfig struct {
	Name string `envconfig:"BROKER_NAME" required:"true"`
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

	r := &Reconciler{
		name:          env.Name,
		triggerLister: triggerInformer.Lister().Triggers(system.Namespace()),
		logger:        logging.FromContext(ctx),
		isReady:       &atomic.Value{},
	}
	r.isReady.Store(false)

	httpTransport, err := cloudevents.NewHTTP(cloudevents.WithGetHandlerFunc(r.getHandler), cloudevents.WithMiddleware(pkgtracing.HTTPSpanIgnoringPaths(readyz)))
	if err != nil {
		log.Fatal("Failed to create cloudevents http protocol", zap.Error(err))
	}
	httpTransport.Handler = http.NewServeMux()
	httpTransport.Handler.HandleFunc(healthz, r.healthZ)
	httpTransport.Handler.HandleFunc(readyz, r.readyZ)

	ceClient, err := cloudevents.NewClient(httpTransport)
	if err != nil {
		log.Fatal("Failed to create cloudevents client", zap.Error(err))
	}
	r.ceClient = ceClient

	impl := brokerreconciler.NewImpl(ctx, r, BrokerClass, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			SkipStatusUpdates: true,
			Concurrency:       1,
		}
	})

	logging.FromContext(ctx).Info("Setting up event handlers")

	brokerInformer.Informer().AddEventHandler(cache.FilteringResourceEventHandler{
		FilterFunc: pkgreconciler.AnnotationFilterFunc(brokerreconciler.ClassAnnotationKey, BrokerClass, false /*allowUnset*/),
		Handler:    controller.HandleAll(impl.Enqueue),
	})

	triggerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if trigger, ok := obj.(*eventingv1.Trigger); ok {
				// Namespace filtering
				if trigger.Namespace != system.Namespace() {
					return
				}
				broker, err := brokerInformer.Lister().Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
				if err != nil {
					log.Print("Failed to lookup Broker for Trigger", zap.Error(err))
				} else {
					label := broker.ObjectMeta.Annotations[brokerreconciler.ClassAnnotationKey]
					if label == BrokerClass {
						impl.Enqueue(broker)
					}
				}
			}
		},
	))

	go func() {
		if err := r.Start(ctx); err != nil {
			log.Fatal("Failed to start dataplane", zap.Error(err))
		}
	}()

	return impl
}
