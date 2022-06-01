/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"net/http"

	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"
	"tableflip.dev/cyanogaster/pkg/reconciler/broker"
)

func main() {
	go func() {
		// Trying to pass Knative health checks.
		_ = http.ListenAndServe(":8080", http.DefaultServeMux)
	}()

	sharedmain.MainWithContext(signals.NewContext(), "controller",
		broker.NewController,
	)
}
