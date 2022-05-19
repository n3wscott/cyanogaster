/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"knative.dev/pkg/logging"
	"knative.dev/pkg/signals"

	"tableflip.dev/cyanogaster/pkg/broker"
)

func main() {
	ctx := signals.NewContext()
	logger := logging.FromContext(ctx)

	b, err := broker.NewBroker(logger)
	if err != nil {
		logger.Error(err)
	}
	if err := b.Start(ctx); err != nil {
		logger.Error(err)
	}
}
