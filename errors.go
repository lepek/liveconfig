package liveconfig

import "errors"

// Sentinel errors returned by Provider and Store operations.
var (
	// ErrBootstrapField is returned when attempting to update a field tagged dyn:"bootstrap".
	// Bootstrap fields are loaded once at startup and cannot be changed at runtime.
	ErrBootstrapField = errors.New("liveconfig: field is bootstrap and cannot be overridden at runtime")

	// ErrSecretField is returned when attempting to update a field tagged dyn:"secret".
	// Secret fields are excluded from the catalog and must never be persisted in the store.
	ErrSecretField = errors.New("liveconfig: field is secret and cannot be stored")

	// ErrUnknownKey is returned when a key is not present in the struct catalog.
	ErrUnknownKey = errors.New("liveconfig: unknown config key")

	// ErrInvalidValue is returned when a raw string value cannot be parsed into
	// the target field's Go type.
	ErrInvalidValue = errors.New("liveconfig: invalid value for field type")

	// ErrProviderClosed is returned when an operation is attempted on a closed Provider.
	ErrProviderClosed = errors.New("liveconfig: provider is closed")

	// ErrNotStruct is returned when the type parameter T is not a struct.
	ErrNotStruct = errors.New("liveconfig: config type must be a struct")
)
