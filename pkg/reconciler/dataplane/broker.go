/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package dataplane

import (
	"context"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"
	"k8s.io/apimachinery/pkg/labels"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	brokerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/broker"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
)

type Reconciler struct {
	// name of the broker
	name string

	// listers index properties about resources
	triggerLister eventinglisters.TriggerNamespaceLister

	mux      sync.Mutex
	triggers []*eventingv1.Trigger
	broker   *eventingv1.Broker

	// Handler fields

	logger   *zap.SugaredLogger
	ceClient cloudevents.Client
	isReady  *atomic.Value
}

// Check that our Reconciler implements Interface
var _ brokerreconciler.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, o *eventingv1.Broker) pkgreconciler.Event {
	logging.FromContext(ctx).Infow("Reconciling", zap.Any("Broker", o.Name), zap.Any("Name", r.name))
	if o.Name != r.name {
		return nil
	}
	logging.FromContext(ctx).Infow("Reconciling", zap.Any("Broker", o))

	triggers, err := r.triggerLister.List(labels.Everything())
	if err != nil {
		return err
	}

	r.mux.Lock()
	r.broker = o
	r.triggers = triggers

	for _, t := range r.triggers {
		logging.FromContext(ctx).Infof("%s --> %s", t.Name, t.Status.SubscriberURI)
	}
	r.mux.Unlock()

	return nil
}
