package liveconfig

import (
	"encoding/json"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"
)

// dyn tag constants - the canonical values for the dyn struct tag.
const (
	dynLive      = "live"
	dynRecreate  = "recreate-on-change"
	dynRestart   = "restart-required"
	dynBootstrap = "bootstrap"
	dynSecret    = "secret"
)

// validDynValues lists every value the dyn tag may take. It is used by
// buildCatalog to fail fast on typos like dyn:"life" instead of silently
// ignoring the field.
var validDynValues = map[string]bool{
	dynLive:      true,
	dynRecreate:  true,
	dynRestart:   true,
	dynBootstrap: true,
	dynSecret:    true,
}

// buildCatalog uses reflection to walk the struct type T and return a
// FieldDescriptor for every field tagged with a dynamic dyn value
// (live, recreate-on-change, or restart-required).
//
// Fields tagged dyn:"bootstrap" or dyn:"secret", or with no dyn tag, are
// excluded from the catalog.
//
// buildCatalog returns an error if:
//   - T is not a struct (pointer-to-struct is rejected; pass the value type).
//   - Any field has a non-empty dyn value that is not in validDynValues.
//   - Any leaf field with a dynamic strategy uses an unsupported Go type.
//
// Failing fast at startup avoids subtle production issues like a typo in
// dyn:"life" silently excluding a field that ops expected to be live.
func buildCatalog[T any]() ([]FieldDescriptor, error) {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		return nil, fmt.Errorf("%w: type is nil", ErrNotStruct)
	}
	if t.Kind() == reflect.Pointer {
		return nil, fmt.Errorf("%w: got pointer type %s; pass the struct value type instead", ErrNotStruct, t)
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("%w: got %s", ErrNotStruct, t.Kind())
	}
	var out []FieldDescriptor
	if err := walkStruct(t, nil, "", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// walkStruct recurses through a struct type collecting dynamic FieldDescriptors.
func walkStruct(t reflect.Type, indices []int, prefix string, out *[]FieldDescriptor) error {
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		if !sf.IsExported() {
			continue
		}

		ft := sf.Type
		if ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}

		keyPart := jsonKeyName(sf)
		if keyPart == "-" {
			continue
		}

		fullKey := keyPart
		if prefix != "" {
			fullKey = prefix + "." + keyPart
		}

		// slices.Clone protects walkStruct's siblings from observing an
		// appended element through a shared backing array.
		idxPath := append(slices.Clone(indices), i)

		dynRaw := strings.TrimSpace(sf.Tag.Get("dyn"))
		if dynRaw != "" && !validDynValues[dynRaw] {
			return fmt.Errorf("liveconfig: field %q has unknown dyn value %q (valid: live, recreate-on-change, restart-required, bootstrap, secret)", fullKey, dynRaw)
		}

		// Nested struct: recurse unless the whole subtree is tagged secret.
		// time.Time is treated as a leaf, not a struct to recurse into.
		if ft.Kind() == reflect.Struct && ft != reflect.TypeOf(time.Time{}) {
			if dynRaw == dynSecret {
				continue
			}
			if err := walkStruct(ft, idxPath, fullKey, out); err != nil {
				return err
			}
			continue
		}

		// Leaf field: only include when tagged with a dynamic strategy.
		strategy, dynamic := parseDynTag(dynRaw)
		if !dynamic {
			continue
		}

		if !isSupportedLeafType(ft) {
			return fmt.Errorf("liveconfig: field %q has dynamic tag %q but unsupported Go type %s (see docs/ANNOTATIONS.md for the supported list)", fullKey, dynRaw, ft)
		}

		*out = append(*out, FieldDescriptor{
			Key:            fullKey,
			Description:    strings.TrimSpace(sf.Tag.Get("desc")),
			TypeName:       goTypeName(ft),
			ReloadStrategy: strategy,
			fieldIndices:   idxPath,
			fieldType:      ft,
		})
	}
	return nil
}

// parseDynTag returns the ReloadStrategy for a dyn tag value.
// Returns ("", false) for bootstrap, secret, absent, or unknown values.
// Callers are expected to have already validated the value against
// validDynValues; this function only distinguishes "dynamic" from
// "non-dynamic" strategies.
func parseDynTag(raw string) (ReloadStrategy, bool) {
	switch raw {
	case dynLive:
		return ReloadStrategyLive, true
	case dynRecreate:
		return ReloadStrategyRecreate, true
	case dynRestart:
		return ReloadStrategyRestart, true
	default:
		return "", false
	}
}

// isSupportedLeafType reports whether the catalog can parse a raw string
// override into the given Go type. The list intentionally mirrors the cases
// handled by parseFieldValue.
func isSupportedLeafType(t reflect.Type) bool {
	if t == reflect.TypeOf(time.Duration(0)) {
		return true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	case reflect.Slice:
		return t.Elem().Kind() == reflect.String
	}
	return false
}

// jsonKeyName returns the JSON key for a struct field: the first segment of
// the json tag, or the lowercase field name when no tag is present.
func jsonKeyName(sf reflect.StructField) string {
	tag := sf.Tag.Get("json")
	if tag == "" {
		return strings.ToLower(sf.Name)
	}
	name, _, _ := strings.Cut(tag, ",")
	if name == "" {
		return strings.ToLower(sf.Name)
	}
	return name
}

// goTypeName returns a human-readable type name for documentation and catalog output.
func goTypeName(t reflect.Type) string {
	if t == reflect.TypeOf(time.Duration(0)) {
		return "time.Duration"
	}
	if t == reflect.TypeOf(time.Time{}) {
		return "time.Time"
	}
	if t.Kind() == reflect.Slice {
		return "[]" + goTypeName(t.Elem())
	}
	return t.String()
}

// navigateField follows the index path through nested structs and returns the
// target reflect.Value. The root must be addressable (e.g. reflect.ValueOf(&s).Elem()).
func navigateField(root reflect.Value, indices []int) reflect.Value {
	v := root
	for _, idx := range indices {
		if v.Kind() == reflect.Pointer {
			if v.IsNil() {
				v.Set(reflect.New(v.Type().Elem()))
			}
			v = v.Elem()
		}
		v = v.Field(idx)
	}
	return v
}

// parseFieldValue converts a raw string value into a reflect.Value whose type
// matches t. Supported types are listed in isSupportedLeafType.
func parseFieldValue(raw string, t reflect.Type) (reflect.Value, error) {
	// time.Duration is int64 underneath but must be parsed with time.ParseDuration.
	if t == reflect.TypeOf(time.Duration(0)) {
		d, err := time.ParseDuration(raw)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%w: cannot parse %q as time.Duration: %v", ErrInvalidValue, raw, err)
		}
		return reflect.ValueOf(d), nil
	}

	switch t.Kind() {
	case reflect.String:
		return reflect.ValueOf(raw).Convert(t), nil

	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%w: cannot parse %q as bool: %v", ErrInvalidValue, raw, err)
		}
		return reflect.ValueOf(b).Convert(t), nil

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%w: cannot parse %q as int: %v", ErrInvalidValue, raw, err)
		}
		return reflect.ValueOf(n).Convert(t), nil

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%w: cannot parse %q as uint: %v", ErrInvalidValue, raw, err)
		}
		return reflect.ValueOf(n).Convert(t), nil

	case reflect.Float32, reflect.Float64:
		f, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%w: cannot parse %q as float: %v", ErrInvalidValue, raw, err)
		}
		return reflect.ValueOf(f).Convert(t), nil

	case reflect.Slice:
		if t.Elem().Kind() != reflect.String {
			return reflect.Value{}, fmt.Errorf("%w: unsupported slice element type %s", ErrInvalidValue, t.Elem())
		}
		parts, err := parseStringSlice(raw)
		if err != nil {
			return reflect.Value{}, fmt.Errorf("%w: %v", ErrInvalidValue, err)
		}
		return reflect.ValueOf(parts), nil

	default:
		return reflect.Value{}, fmt.Errorf("%w: unsupported field type %s", ErrInvalidValue, t)
	}
}

// parseStringSlice accepts two input shapes:
//
//   - a JSON array of strings, e.g. ["a","b","c"], which is decoded via
//     encoding/json so quoting rules behave like real JSON.
//   - a comma-separated list, e.g. a,b,c, which trims surrounding whitespace
//     around each element and skips empty entries.
//
// The JSON form is preferred when values may contain commas or special
// characters; the comma-separated form is convenient for simple lists.
func parseStringSlice(raw string) ([]string, error) {
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "[") {
		var out []string
		if err := json.Unmarshal([]byte(trimmed), &out); err != nil {
			return nil, fmt.Errorf("invalid JSON string slice %q: %w", raw, err)
		}
		return out, nil
	}
	if trimmed == "" {
		return nil, nil
	}
	var out []string
	for _, p := range strings.Split(trimmed, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out, nil
}

// applyOverrides returns a copy of base with the given overrides applied.
// Fields that fail to parse are skipped; onErr is called with the error so the
// caller can log a warning without breaking the snapshot rebuild.
func applyOverrides[T any](base T, overrides map[string]string, catalog []FieldDescriptor, onErr func(key, raw string, err error)) T {
	result := base
	rv := reflect.ValueOf(&result).Elem()

	for i := range catalog {
		desc := &catalog[i]
		raw, ok := overrides[desc.Key]
		if !ok {
			continue
		}
		parsed, err := parseFieldValue(raw, desc.fieldType)
		if err != nil {
			if onErr != nil {
				onErr(desc.Key, raw, err)
			}
			continue
		}
		field := navigateField(rv, desc.fieldIndices)
		if !field.IsValid() || !field.CanSet() {
			continue
		}
		field.Set(parsed)
	}
	return result
}
