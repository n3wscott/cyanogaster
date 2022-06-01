/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package dataplane

import (
	"context"
	"errors"
	"github.com/cloudevents/sdk-go/v2/protocol/gochan"
	"log"
	"net/http"
	"strings"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/rickb777/date/period"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	eventingduckv1 "knative.dev/eventing/pkg/apis/duck/v1"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/logging"
)

const (
	writeTimeout = 15 * time.Minute

	readyz  = "/readyz"
	healthz = "/healthz"
)

// FilterResult has the result of the filtering operation.
type FilterResult string

func (r *Reconciler) healthZ(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

func (r *Reconciler) readyZ(writer http.ResponseWriter, _ *http.Request) {
	if r.isReady == nil || !r.isReady.Load().(bool) {
		http.Error(writer, http.StatusText(http.StatusServiceUnavailable), http.StatusServiceUnavailable)
		return
	}
	writer.WriteHeader(http.StatusOK)
}

// Start begins to receive messages for the handler.
//
// Only HTTP POST requests to the root path (/) are accepted. If other paths or
// methods are needed, use the HandleRequest method directly with another HTTP
// server.
//
// This method will block until a message is received on the stop channel.
func (r *Reconciler) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	ce, err := cloudevents.NewClient(gochan.New())
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}
	r.ceChan = ce

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.ceClient.StartReceiver(ctx, r.ingress)
	}()
	go func() {
		errCh <- r.ceChan.StartReceiver(ctx, r.receiver)
	}()

	// We are ready.
	r.isReady.Store(true)

	// Stop either if the receiver stops (sending to errCh) or if stopCh is closed.
	select {
	case err := <-errCh:
		return err
	case <-ctx.Done():
		break
	}

	// No longer ready.
	r.isReady.Store(false)

	// stopCh has been closed, we need to gracefully shutdown h.ceClient. cancel() will start its
	// shutdown, if it hasn't finished in a reasonable amount of time, just return an error.
	cancel()
	select {
	case err := <-errCh:
		return err
	case <-time.After(writeTimeout):
		return errors.New("timeout shutting down ceClient")
	}
}

func (r *Reconciler) getHandler(resp http.ResponseWriter, req *http.Request) {
	_, _ = resp.Write([]byte("hello"))
}

func (r *Reconciler) ingress(ctx context.Context, event cloudevents.Event) error {
	r.logger.Infof("%s", event)
	if result := r.ceChan.Send(ctx, event); cloudevents.IsUndelivered(result) {
		r.logger.Errorw("failed to send event", zap.Error(result))
		return cloudevents.NewHTTPResult(500, "unable to ingress")
	}
	return nil
}

// sendEvent sends an event to a subscriber if the trigger filter passes.
func (r *Reconciler) receiver(ctx context.Context, event cloudevents.Event) error {
	r.logger.Infof("%s", event)

	if r.broker != nil && r.broker.Spec.Delivery != nil && r.broker.Spec.Delivery.BackoffPolicy != nil {
		retry := 5
		if r.broker.Spec.Delivery.Retry != nil {
			retry = int(*r.broker.Spec.Delivery.Retry)
		}
		backoff := time.Millisecond * 10
		if r.broker.Spec.Delivery.BackoffDelay != nil {
			p, _ := period.Parse(*r.broker.Spec.Delivery.BackoffDelay)
			backoff = p.DurationApprox()
		}

		if *r.broker.Spec.Delivery.BackoffPolicy == eventingduckv1.BackoffPolicyLinear {
			ctx = cloudevents.ContextWithRetriesLinearBackoff(ctx, backoff, retry)
		} else {
			ctx = cloudevents.ContextWithRetriesExponentialBackoff(ctx, backoff, retry)
		}
	}

	// Quickly collect the current matching triggers.
	var triggers []*eventingv1.Trigger
	r.mux.Lock()
	r.logger.Info("Triggers:", len(r.triggers))
	for _, trigger := range r.triggers {
		if eventMatchesFilter(ctx, &event, trigger.Spec.Filter.Attributes) {
			r.logger.Infof("matched! [%s] -> %s", event.ID(), trigger.Status.SubscriberURI.URL().String())
			triggers = append(triggers, trigger)
		} else {
			r.logger.Infof("no match. [%s]", event.ID())
		}
	}
	r.mux.Unlock()

	// Then process the matching triggers one at a time.
	for _, trigger := range triggers {
		// TODO: this could be go routines and a worker pool here to not let a single trigger block the others.
		sendingCTX := cloudevents.ContextWithTarget(ctx, trigger.Status.SubscriberURI.URL().String())
		sendingCTX = trace.NewContext(sendingCTX, trace.FromContext(ctx))

		if reply, result := r.ceClient.Request(sendingCTX, event); cloudevents.IsUndelivered(result) {
			r.logger.Errorw("failed to send event", zap.Error(result))

			// DLQ
			if r.broker.Status.DeadLetterSinkURI != nil {
				go func() {
					dlqCTX := cloudevents.ContextWithTarget(ctx, r.broker.Status.DeadLetterSinkURI.URL().String())
					if result := r.ceClient.Send(dlqCTX, event); cloudevents.IsUndelivered(result) {
						r.logger.Errorw("failed to dql", zap.Error(result))
					}
				}()
			}
			// TODO: DLQ overrides from trigger.

		} else if reply != nil {
			// OMG so much yolo...
			go func() {
				sendingCTX := cloudevents.ContextWithTarget(ctx, "http://loalhost:8080") // TODO should be this ingress?
				sendingCTX = trace.NewContext(sendingCTX, trace.FromContext(ctx))
				if result := r.ceClient.Send(sendingCTX, *reply); cloudevents.IsUndelivered(result) {
					r.logger.Errorw("failed to send reply", zap.Error(result))
				}
			}()
		}
	}
	return nil
}

func eventMatchesFilter(ctx context.Context, event *cloudevents.Event, attributesFilter eventingv1.TriggerFilterAttributes) bool {
	for a, v := range attributesFilter {
		a = strings.ToLower(a)
		logging.FromContext(ctx).Info("filtering on", a)
		// Find the value.
		var ev string
		switch a {
		case "specversion":
			ev = event.SpecVersion()
		case "type":
			ev = event.Type()
		case "source":
			ev = event.Source()
		case "subject":
			ev = event.Subject()
		case "id":
			ev = event.ID()
		case "time":
			ev = event.Time().String()
		case "schemaurl":
			ev = event.DataSchema()
		case "datacontenttype":
			ev = event.DataContentType()
		case "datamediatype":
			ev = event.DataMediaType()
		default:
			if exv, ok := event.Extensions()[a]; ok {
				ev, _ = types.ToString(exv)
			}
		}
		// Compare
		if v != ev {
			return false
		}
	}
	return true
}
