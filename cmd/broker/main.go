/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package main

import (
	"knative.dev/pkg/injection/sharedmain"
	"knative.dev/pkg/signals"
	"tableflip.dev/cyanogaster/pkg/reconciler/dataplane"
)

func main() {
	sharedmain.MainWithContext(signals.NewContext(), "dataplane",
		dataplane.NewController,
	)
}
