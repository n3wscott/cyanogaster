/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package broker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/types"
	"github.com/kelseyhightower/envconfig"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	eventingv1 "knative.dev/eventing/pkg/apis/eventing/v1"
	"knative.dev/pkg/logging"
	pkgtracing "knative.dev/pkg/tracing"
	"tableflip.dev/cyanogaster/pkg/reconciler/broker/resources"
)

const (
	writeTimeout = 15 * time.Minute

	readyz  = "/readyz"
	healthz = "/healthz"
)

// Handler parses Cloud Events, determines if they pass a filter, and sends them to a subscriber.
type Handler struct {
	logger   *zap.SugaredLogger
	ceClient cloudevents.Client
	isReady  *atomic.Value

	TriggersJson string `envconfig:"TRIGGERS" required:"true"`
	triggers     []resources.Trigger
}

// FilterResult has the result of the filtering operation.
type FilterResult string

func NewBroker(logger *zap.SugaredLogger) (*Handler, error) {
	r := &Handler{
		logger:  logger,
		isReady: &atomic.Value{},
	}
	r.isReady.Store(false)

	httpTransport, err := cloudevents.NewHTTP(cloudevents.WithGetHandlerFunc(r.getHandler), cloudevents.WithMiddleware(pkgtracing.HTTPSpanIgnoringPaths(readyz)))
	if err != nil {
		return nil, err
	}

	ceClient, err := cloudevents.NewClient(httpTransport)
	if err != nil {
		return nil, err
	}
	r.ceClient = ceClient

	if err := envconfig.Process("", r); err != nil {
		return nil, err
	}

	r.triggers = make([]resources.Trigger, 0)
	if err := json.Unmarshal([]byte(r.TriggersJson), &r.triggers); err != nil {
		return nil, err
	}

	httpTransport.Handler = http.NewServeMux()
	httpTransport.Handler.HandleFunc(healthz, r.healthZ)
	httpTransport.Handler.HandleFunc(readyz, r.readyZ)

	return r, nil
}

func (r *Handler) healthZ(writer http.ResponseWriter, _ *http.Request) {
	writer.WriteHeader(http.StatusOK)
}

func (r *Handler) readyZ(writer http.ResponseWriter, _ *http.Request) {
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
func (r *Handler) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- r.ceClient.StartReceiver(ctx, r.receiver)
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

func (r *Handler) getHandler(resp http.ResponseWriter, req *http.Request) {
	_, _ = resp.Write([]byte("hello"))
}

// sendEvent sends an event to a subscriber if the trigger filter passes.
func (r *Handler) receiver(ctx context.Context, event cloudevents.Event) error {
	r.logger.Infof("%s", event)

	for _, trigger := range r.triggers {
		if eventMatchesFilter(ctx, &event, trigger.AttributesFilter) {
			r.logger.Infof("matched! [%s] -> %s", event.ID(), trigger.Subscriber.URL().String())
			sendingCTX := cloudevents.ContextWithTarget(ctx, trigger.Subscriber.URL().String())
			sendingCTX = trace.NewContext(sendingCTX, trace.FromContext(ctx))

			if reply, result := r.ceClient.Request(sendingCTX, event); cloudevents.IsUndelivered(result) {
				r.logger.Errorw("failed to send event", zap.Error(result))
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
