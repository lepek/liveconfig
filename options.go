package liveconfig

import (
	"log/slog"
	"time"
)

const (
	defaultNamespace       = "default"
	defaultRefreshInterval = 5 * time.Minute
	defaultSubscriberBuf   = 16
)

type providerOptions struct {
	namespace        string
	logger           *slog.Logger
	refreshInterval  time.Duration
	subscriberBuffer int
}

func defaultProviderOptions() providerOptions {
	return providerOptions{
		namespace:        defaultNamespace,
		logger:           slog.Default(),
		refreshInterval:  defaultRefreshInterval,
		subscriberBuffer: defaultSubscriberBuf,
	}
}

// Option configures a Provider at construction time.
type Option func(*providerOptions)

// WithNamespace sets the store namespace used to scope all config keys.
// This is typically the service name (e.g. "poseidon", "poseidon-api").
// All Store operations performed by the Provider use this namespace.
//
// Default: "default".
func WithNamespace(ns string) Option {
	return func(o *providerOptions) { o.namespace = ns }
}

// WithLogger sets the structured logger used by the Provider for info, warning,
// and error messages. Pass slog.New(...) to route logs to your preferred handler.
//
// Passing nil is treated as "use the default" rather than as a request to
// silence logging; pass a slog.Logger backed by an io.Discard handler to
// disable output explicitly.
//
// Default: slog.Default().
func WithLogger(logger *slog.Logger) Option {
	return func(o *providerOptions) {
		if logger == nil {
			logger = slog.Default()
		}
		o.logger = logger
	}
}

// WithRefreshInterval sets how often the Provider performs a full reload of all
// overrides from the store, regardless of NOTIFY/Watch events. This acts as a
// safety net for changes that may have been missed during a connection drop.
//
// Values <= 0 are treated as "use the default".
//
// Default: 5 minutes.
func WithRefreshInterval(d time.Duration) Option {
	return func(o *providerOptions) {
		if d <= 0 {
			d = defaultRefreshInterval
		}
		o.refreshInterval = d
	}
}

// WithSubscriberBuffer sets the buffer size of the channels returned by
// Provider.Subscribe. If a subscriber does not read fast enough and the buffer
// fills up, the event is dropped and a warning is logged. Increase this value
// if your recreate-on-change handler is slow.
//
// Values <= 0 are treated as "use the default".
//
// Default: 16.
func WithSubscriberBuffer(n int) Option {
	return func(o *providerOptions) {
		if n <= 0 {
			n = defaultSubscriberBuf
		}
		o.subscriberBuffer = n
	}
}
