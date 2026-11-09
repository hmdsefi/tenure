// Copyright 2026 Hamed Yousefi
// SPDX-License-Identifier: MPL-2.0

package k8s

import (
	"errors"

	"k8s.io/client-go/rest"
)

// Request limits and user agent RESTConfig sets where the configuration leaves them unset
// (K8S-700).
const (
	defaultQPS       = 50
	defaultBurst     = 100
	userAgentProduct = "tenure-k8s"
)

// RESTConfig returns a copy of c prepared for an Adapter's client: QPS 50 and Burst 100 where
// c leaves them zero, and a user agent that starts with "tenure-k8s" where c sets none. It
// never changes c and makes no request. When err is not nil, RESTConfig returns nil and err,
// so it can wrap a function that returns a configuration and an error:
//
//	cfg, err := k8s.RESTConfig(rest.InClusterConfig())
func RESTConfig(c *rest.Config, err error) (*rest.Config, error) {
	if err != nil {
		return nil, err
	}
	if c == nil {
		return nil, errors.New("k8s: nil rest.Config")
	}
	c = rest.CopyConfig(c)
	if c.QPS == 0 {
		c.QPS = defaultQPS
	}
	if c.Burst == 0 {
		c.Burst = defaultBurst
	}
	if c.UserAgent == "" {
		c.UserAgent = userAgentProduct + " " + rest.DefaultKubernetesUserAgent()
	}
	return c, nil
}
