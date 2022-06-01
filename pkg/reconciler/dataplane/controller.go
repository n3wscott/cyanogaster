/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package dataplane

import (
	"context"
	"k8s.io/apimachinery/pkg/labels"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	"knative.dev/pkg/resolver"
	"knative.dev/pkg/system"
	"log"
	"net/http"
	"sync/atomic"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/broker"
	triggerinformer "knative.dev/eventing/pkg/client/injection/informers/eventing/v1/trigger"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	"knative.dev/pkg/configmap"
	"knative.dev/pkg/controller"
	"knative.dev/pkg/logging"
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
		brokerLister: brokerInformer.Lister(),
		brokerClass:  BrokerClass,
		name:         env.Name,
		logger:       logging.FromContext(ctx),
		isReady:      &atomic.Value{},
		triggers:     make(map[string]*eventingv1.Trigger),
	}
	r.isReady.Store(false)

	logging.FromContext(ctx).Info("Setting up event handlers")

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

	impl := triggerreconciler.NewImpl(ctx, r, func(impl *controller.Impl) controller.Options {
		return controller.Options{
			Concurrency: 1,
			PromoteFilterFunc: func(obj interface{}) bool {
				if trigger, ok := obj.(*eventingv1.Trigger); ok {
					if trigger.Namespace != system.Namespace() || trigger.Spec.Broker != env.Name {
						return false
					}
					broker, err := brokerInformer.Lister().Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
					if err != nil {
						log.Print("Failed to lookup Broker for Trigger", zap.Error(err))
					} else {
						label := broker.ObjectMeta.Annotations[brokerreconciler.ClassAnnotationKey]
						return label == BrokerClass
					}
				}
				return false
			},
		}
	})
	r.uriResolver = resolver.NewURIResolverFromTracker(ctx, impl.Tracker)

	logging.FromContext(ctx).Info("Setting up event handlers")

	brokerInformer.Informer().AddEventHandler(controller.HandleAll(
		func(obj interface{}) {
			if broker, ok := obj.(*eventingv1.Broker); ok {
				if broker.Namespace != system.Namespace() {
					return
				}
				label := broker.ObjectMeta.Annotations[brokerreconciler.ClassAnnotationKey]
				if label != BrokerClass || env.Name != broker.Name {
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
				if trigger.Namespace != system.Namespace() || trigger.Spec.Broker != env.Name {
					return
				}

				// Observe deletions directly.
				if trigger.DeletionTimestamp != nil {
					r.removeTrigger(context.Background(), trigger)
				}

				broker, err := brokerInformer.Lister().Brokers(trigger.Namespace).Get(trigger.Spec.Broker)
				if err != nil {
					log.Print("Failed to lookup Broker for Trigger", zap.Error(err))
				} else {
					label := broker.ObjectMeta.Annotations[brokerreconciler.ClassAnnotationKey]
					if label != BrokerClass {
						return
					}
					impl.Enqueue(obj)
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
