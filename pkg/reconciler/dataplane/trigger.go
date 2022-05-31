/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package dataplane

import (
	"context"
	"knative.dev/pkg/system"
	"sync"
	"sync/atomic"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"go.uber.org/zap"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
)

type Reconciler struct {
	// name of the broker
	name         string
	brokerClass  string
	brokerLister eventinglisters.BrokerLister
	uriResolver  *resolver.URIResolver

	mux      sync.Mutex
	triggers map[string]*eventingv1.Trigger
	broker   *eventingv1.Broker

	// Handler fields

	ceChan   cloudevents.Client
	logger   *zap.SugaredLogger
	ceClient cloudevents.Client
	isReady  *atomic.Value
}

// HACK HACK HACK
// this is going to manage custom a custom condition set for these triggers.

var triggerCondSet = apis.NewLivingConditionSet(
	//eventingv1.TriggerConditionBroker,
	eventingv1.TriggerConditionSubscriberResolved)

func triggerInitializeConditions(ts *eventingv1.TriggerStatus) {
	ts.Conditions = nil
	triggerCondSet.Manage(ts).InitializeConditions()
}

func triggerMarkSubscriberResolvedSucceeded(ts *eventingv1.TriggerStatus) {
	triggerCondSet.Manage(ts).MarkTrue(eventingv1.TriggerConditionSubscriberResolved)
}

func triggerMarkSubscriberResolvedFailed(ts *eventingv1.TriggerStatus, reason, messageFormat string, messageA ...interface{}) {
	triggerCondSet.Manage(ts).MarkFalse(eventingv1.TriggerConditionSubscriberResolved, reason, messageFormat, messageA...)
}

// Check that our Reconciler implements Interface
var _ triggerreconciler.Interface = (*Reconciler)(nil)
var _ triggerreconciler.Finalizer = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, o *eventingv1.Trigger) pkgreconciler.Event {
	logging.FromContext(ctx).Info("Reconciling", zap.Any("Trigger", o))

	triggerInitializeConditions(&o.Status)

	logging.FromContext(ctx).Infof("Reconciling Trigger: %s", o.Name)

	if o.Spec.Subscriber.Ref != nil && o.Spec.Subscriber.Ref.Namespace == "" {
		// To call URIFromDestinationV1(ctx context.Context, dest v1.Destination, parent interface{}), dest.Ref must have a Namespace
		// If Subscriber.Ref.Namespace is nil, We will use the Namespace of Trigger as the Namespace of dest.Ref
		o.Spec.Subscriber.Ref.Namespace = o.GetNamespace()
	}

	subscriberURI, err := r.uriResolver.URIFromDestinationV1(ctx, o.Spec.Subscriber, o)
	if err != nil {
		logging.FromContext(ctx).Errorw("Unable to get the Subscriber's URI", zap.Error(err))
		triggerMarkSubscriberResolvedFailed(&o.Status, "Unable to get the Subscriber's URI", "%v", err)
		o.Status.SubscriberURI = nil
		return err
	}
	o.Status.SubscriberURI = subscriberURI
	triggerMarkSubscriberResolvedSucceeded(&o.Status)

	if b, err := r.brokerLister.Brokers(o.Namespace).Get(o.Spec.Broker); err == nil && b != nil {
		r.addBroker(ctx, b)
	}
	r.addTrigger(ctx, o)

	return nil
}

func (r *Reconciler) FinalizeKind(ctx context.Context, o *eventingv1.Trigger) pkgreconciler.Event {
	r.removeTrigger(ctx, o)

	return nil
}

func (r *Reconciler) removeTrigger(ctx context.Context, o *eventingv1.Trigger) {
	r.mux.Lock()
	delete(r.triggers, o.Name)
	r.mux.Unlock()

	for _, t := range r.triggers {
		logging.FromContext(ctx).Infof("%s --> %s", t.Name, t.Status.SubscriberURI)
	}
}

func (r *Reconciler) addTrigger(ctx context.Context, o *eventingv1.Trigger) {
	if !o.Status.IsReady() {
		return
	}
	if o.Namespace != system.Namespace() || (r.broker != nil && o.Spec.Broker != r.broker.Name) {
		return
	}

	logging.FromContext(ctx).Infof("Adding %s[ns: %s][trigger.Broker: %s][reconciler.Broker: %s] system:%s", o.Name, o.Namespace, o.Spec.Broker, r.name, system.Namespace())

	r.mux.Lock()
	r.triggers[o.Name] = o
	r.mux.Unlock()

	for _, t := range r.triggers {
		logging.FromContext(ctx).Infof("%s --> %s", t.Name, t.Status.SubscriberURI)
	}
}

func (r *Reconciler) addBroker(ctx context.Context, o *eventingv1.Broker) {
	if o.Namespace != system.Namespace() {
		return
	}

	r.mux.Lock()
	r.broker = o
	r.mux.Unlock()
}
