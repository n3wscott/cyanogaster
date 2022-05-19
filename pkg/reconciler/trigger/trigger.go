/*
Copyright 2021 The Knative Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package trigger

import (
	"context"

	"go.uber.org/zap"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	triggerreconciler "knative.dev/eventing/pkg/client/injection/reconciler/eventing/v1/trigger"
	eventinglisters "knative.dev/eventing/pkg/client/listers/eventing/v1"
	"knative.dev/eventing/pkg/duck"
	"knative.dev/pkg/apis"
	"knative.dev/pkg/logging"
	pkgreconciler "knative.dev/pkg/reconciler"
	"knative.dev/pkg/resolver"
)

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

//func triggerPropagateBrokerStatus(ts *eventingv1.TriggerStatus, bs *eventingv1.BrokerStatus) {
//	bc := bs.GetTopLevelCondition()
//	if bc == nil {
//		triggerCondSet.Manage(ts).MarkUnknown(eventingv1.TriggerConditionBroker,
//			"BrokerNotConfigured", "Broker has not yet been reconciled.")
//		return
//	}
//
//	switch {
//	case bc.Status == corev1.ConditionUnknown:
//		triggerCondSet.Manage(ts).MarkUnknown(eventingv1.TriggerConditionBroker, bc.Reason, bc.Message)
//	case bc.Status == corev1.ConditionTrue:
//		triggerCondSet.Manage(ts).MarkTrue(eventingv1.TriggerConditionBroker)
//	case bc.Status == corev1.ConditionFalse:
//		triggerCondSet.Manage(ts).MarkFalse(eventingv1.TriggerConditionBroker, bc.Reason, bc.Message)
//	default:
//		triggerCondSet.Manage(ts).MarkUnknown(eventingv1.TriggerConditionBroker, "BrokerUnknown", "The status of Broker is invalid: %v", bc.Status)
//	}
//}

// -----------------

type Reconciler struct {
	brokerClass  string
	brokerLister eventinglisters.BrokerLister

	// Dynamic tracker to track AddressableTypes. In particular, it tracks Trigger subscribers.
	addressableTracker duck.ListableTracker // TODO: delete this?
	uriResolver        *resolver.URIResolver
}

// Check that our Reconciler implements Interface
var _ triggerreconciler.Interface = (*Reconciler)(nil)

func (r *Reconciler) ReconcileKind(ctx context.Context, o *eventingv1.Trigger) pkgreconciler.Event {
	logging.FromContext(ctx).Info("Reconciling", zap.Any("Trigger", o))

	triggerInitializeConditions(&o.Status)

	logging.FromContext(ctx).Infof("Reconciling Trigger: %s", o.Name)

	var err error

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

	return nil
}
