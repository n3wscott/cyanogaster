//go:build tools
// +build tools

/*
Copyright 2022 Scott Nichols
SPDX-License-Identifier: Apache-2.0
*/

package tools

import (
	_ "knative.dev/hack"

	// codegen: hack/generate-knative.sh
	_ "knative.dev/pkg/hack"
)
