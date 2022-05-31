/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"log"

	"github.com/kelseyhightower/envconfig"
	"go.uber.org/zap"
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"
	"tableflip.dev/cyanogaster/pkg/reconciler/dataplane"
)

type envConfig struct {
	Name string `envconfig:"BROKER_NAME" required:"true"`
}

func main() {
	var env envConfig
	if err := envconfig.Process("", &env); err != nil {
		log.Fatal("Failed to process env var", zap.Error(err))
	}

	sharedmain.MainWithContext(signals.NewContext(), "dataplane-"+env.Name,
		dataplane.NewController,
	)
}
