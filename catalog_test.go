package liveconfig

import (
	"reflect"
	"testing"
	"time"
)

// --- test structs ---

type flatCfg struct {
	Name    string        `json:"name"    dyn:"live"               desc:"Service name"`
	Timeout time.Duration `json:"timeout" dyn:"live"               desc:"HTTP timeout"`
	Retries int           `json:"retries" dyn:"recreate-on-change" desc:"Retry count"`
	Cron    string        `json:"cron"    dyn:"restart-required"`
	Port    int           `json:"port"    dyn:"bootstrap"`
	Secret  string        `json:"secret"  dyn:"secret"`
	NoTag   string        `json:"no_tag"`
}

type nestedCfg struct {
	Host   string  `json:"host"  dyn:"bootstrap"`
	JiraSection jiraSectionCfg `json:"jira"`
}

type jiraSectionCfg struct {
	Epic      string `json:"epic"      dyn:"live"               desc:"Default Jira epic"`
	Component string `json:"component" dyn:"live"`
	ApiKey    string `json:"api_key"   dyn:"secret"`
}

type sliceCfg struct {
	Tags   []string `json:"tags"  dyn:"live" desc:"Comma-separated tags"`
	Owners []string `json:"owners" dyn:"recreate-on-change"`
}

// --- buildCatalog tests ---

func TestBuildCatalog_FlatStruct(t *testing.T) {
	catalog, err := buildCatalog[flatCfg]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	keys := catalogKeys(catalog)

	// Dynamic fields must be present.
	mustContain(t, keys, "name")
	mustContain(t, keys, "timeout")
	mustContain(t, keys, "retries")
	mustContain(t, keys, "cron")

	// Bootstrap, secret, and untagged fields must be excluded.
	mustNotContain(t, keys, "port")
	mustNotContain(t, keys, "secret")
	mustNotContain(t, keys, "no_tag")
}

func TestBuildCatalog_ReloadStrategies(t *testing.T) {
	catalog, err := buildCatalog[flatCfg]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	byKey := catalogByKey(catalog)

	if byKey["name"].ReloadStrategy != ReloadStrategyLive {
		t.Errorf("name: want live, got %s", byKey["name"].ReloadStrategy)
	}
	if byKey["retries"].ReloadStrategy != ReloadStrategyRecreate {
		t.Errorf("retries: want recreate-on-change, got %s", byKey["retries"].ReloadStrategy)
	}
	if byKey["cron"].ReloadStrategy != ReloadStrategyRestart {
		t.Errorf("cron: want restart-required, got %s", byKey["cron"].ReloadStrategy)
	}
}

func TestBuildCatalog_Description(t *testing.T) {
	catalog, err := buildCatalog[flatCfg]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	byKey := catalogByKey(catalog)
	if byKey["name"].Description != "Service name" {
		t.Errorf("name description: got %q", byKey["name"].Description)
	}
	if byKey["cron"].Description != "" {
		t.Errorf("cron description: expected empty, got %q", byKey["cron"].Description)
	}
}

func TestBuildCatalog_NestedStruct(t *testing.T) {
	catalog, err := buildCatalog[nestedCfg]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	keys := catalogKeys(catalog)

	// Nested dynamic fields must use dot-separated keys.
	mustContain(t, keys, "jira.epic")
	mustContain(t, keys, "jira.component")

	// Bootstrap at root and secret inside nested struct must be excluded.
	mustNotContain(t, keys, "host")
	mustNotContain(t, keys, "jira.api_key")
}

func TestBuildCatalog_TypeNames(t *testing.T) {
	catalog, err := buildCatalog[flatCfg]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	byKey := catalogByKey(catalog)
	if byKey["timeout"].TypeName != "time.Duration" {
		t.Errorf("timeout type: want time.Duration, got %s", byKey["timeout"].TypeName)
	}
	if byKey["retries"].TypeName != "int" {
		t.Errorf("retries type: want int, got %s", byKey["retries"].TypeName)
	}
	if byKey["name"].TypeName != "string" {
		t.Errorf("name type: want string, got %s", byKey["name"].TypeName)
	}
}

func TestBuildCatalog_SliceField(t *testing.T) {
	catalog, err := buildCatalog[sliceCfg]()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	keys := catalogKeys(catalog)
	mustContain(t, keys, "tags")
	mustContain(t, keys, "owners")
}

func TestBuildCatalog_NonStructReturnsError(t *testing.T) {
	_, err := buildCatalog[string]()
	if err == nil {
		t.Fatal("expected error for non-struct type, got nil")
	}
}

func TestBuildCatalog_RejectsPointerType(t *testing.T) {
	_, err := buildCatalog[*flatCfg]()
	if err == nil {
		t.Fatal("expected error for pointer-to-struct type, got nil")
	}
}

type typoDynCfg struct {
	// "life" is a typo of "live"; the catalog must refuse it rather than
	// silently skip the field.
	Name string `json:"name" dyn:"life"`
}

func TestBuildCatalog_RejectsUnknownDynValue(t *testing.T) {
	_, err := buildCatalog[typoDynCfg]()
	if err == nil {
		t.Fatal("expected error for unknown dyn value, got nil")
	}
}

type timeLeafCfg struct {
	// time.Time as a leaf with a dynamic strategy is not supported.
	Deadline time.Time `json:"deadline" dyn:"live"`
}

func TestBuildCatalog_RejectsUnsupportedLeafType(t *testing.T) {
	_, err := buildCatalog[timeLeafCfg]()
	if err == nil {
		t.Fatal("expected error for unsupported leaf type, got nil")
	}
}

type mapLeafCfg struct {
	Headers map[string]string `json:"headers" dyn:"live"`
}

func TestBuildCatalog_RejectsMapLeaf(t *testing.T) {
	_, err := buildCatalog[mapLeafCfg]()
	if err == nil {
		t.Fatal("expected error for map leaf type, got nil")
	}
}

// --- parseFieldValue tests ---

func TestParseFieldValue_String(t *testing.T) {
	v, err := parseFieldValue("hello", reflect.TypeOf(""))
	if err != nil {
		t.Fatal(err)
	}
	if v.String() != "hello" {
		t.Errorf("got %q", v.String())
	}
}

func TestParseFieldValue_Bool(t *testing.T) {
	cases := []struct{ raw string; want bool }{
		{"true", true}, {"false", false}, {"1", true}, {"0", false},
	}
	for _, c := range cases {
		v, err := parseFieldValue(c.raw, reflect.TypeOf(false))
		if err != nil {
			t.Fatalf("raw=%q: %v", c.raw, err)
		}
		if v.Bool() != c.want {
			t.Errorf("raw=%q: got %v, want %v", c.raw, v.Bool(), c.want)
		}
	}
}

func TestParseFieldValue_Int(t *testing.T) {
	v, err := parseFieldValue("42", reflect.TypeOf(0))
	if err != nil {
		t.Fatal(err)
	}
	if v.Int() != 42 {
		t.Errorf("got %d", v.Int())
	}
}

func TestParseFieldValue_Duration(t *testing.T) {
	v, err := parseFieldValue("30s", reflect.TypeOf(time.Duration(0)))
	if err != nil {
		t.Fatal(err)
	}
	if v.Interface().(time.Duration) != 30*time.Second {
		t.Errorf("got %v", v.Interface())
	}
}

func TestParseFieldValue_DurationInvalid(t *testing.T) {
	_, err := parseFieldValue("notaduration", reflect.TypeOf(time.Duration(0)))
	if err == nil {
		t.Fatal("expected error for invalid duration")
	}
}

func TestParseFieldValue_StringSliceComma(t *testing.T) {
	v, err := parseFieldValue("a,b,c", reflect.TypeOf([]string{}))
	if err != nil {
		t.Fatal(err)
	}
	got := v.Interface().([]string)
	want := []string{"a", "b", "c"}
	if !equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseFieldValue_StringSliceJSON(t *testing.T) {
	v, err := parseFieldValue(`["x","y","z"]`, reflect.TypeOf([]string{}))
	if err != nil {
		t.Fatal(err)
	}
	got := v.Interface().([]string)
	want := []string{"x", "y", "z"}
	if !equal(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestParseFieldValue_BoolInvalid(t *testing.T) {
	_, err := parseFieldValue("notabool", reflect.TypeOf(false))
	if err == nil {
		t.Fatal("expected error")
	}
}

// --- applyOverrides tests ---

func TestApplyOverrides_Basic(t *testing.T) {
	base := flatCfg{Name: "original", Timeout: 5 * time.Second, Retries: 3}
	catalog, _ := buildCatalog[flatCfg]()

	overrides := map[string]string{
		"name":    "updated",
		"timeout": "10s",
		"retries": "7",
	}

	result := applyOverrides(base, overrides, catalog, nil)

	if result.Name != "updated" {
		t.Errorf("Name: got %q", result.Name)
	}
	if result.Timeout != 10*time.Second {
		t.Errorf("Timeout: got %v", result.Timeout)
	}
	if result.Retries != 7 {
		t.Errorf("Retries: got %d", result.Retries)
	}
}

func TestApplyOverrides_NestedStruct(t *testing.T) {
	base := nestedCfg{JiraSection: jiraSectionCfg{Epic: "OLD-1"}}
	catalog, _ := buildCatalog[nestedCfg]()

	overrides := map[string]string{"jira.epic": "NEW-99"}
	result := applyOverrides(base, overrides, catalog, nil)

	if result.JiraSection.Epic != "NEW-99" {
		t.Errorf("Epic: got %q", result.JiraSection.Epic)
	}
}

func TestApplyOverrides_BaseUnchanged(t *testing.T) {
	base := flatCfg{Name: "original"}
	catalog, _ := buildCatalog[flatCfg]()
	overrides := map[string]string{"name": "changed"}

	_ = applyOverrides(base, overrides, catalog, nil)

	if base.Name != "original" {
		t.Error("base struct was mutated")
	}
}

func TestApplyOverrides_InvalidValueKeepsBase(t *testing.T) {
	base := flatCfg{Retries: 5}
	catalog, _ := buildCatalog[flatCfg]()
	overrides := map[string]string{"retries": "not-an-int"}

	var errKey string
	result := applyOverrides(base, overrides, catalog, func(key, _ string, _ error) {
		errKey = key
	})

	if errKey != "retries" {
		t.Errorf("expected onErr called with 'retries', got %q", errKey)
	}
	if result.Retries != 5 {
		t.Errorf("Retries should remain base value 5, got %d", result.Retries)
	}
}

func TestApplyOverrides_UnknownKeyIgnored(t *testing.T) {
	base := flatCfg{Name: "original"}
	catalog, _ := buildCatalog[flatCfg]()
	overrides := map[string]string{"does_not_exist": "value"}

	result := applyOverrides(base, overrides, catalog, nil)
	if result.Name != "original" {
		t.Error("base should be unchanged when override key is unknown")
	}
}

// --- helpers ---

func catalogKeys(c []FieldDescriptor) []string {
	out := make([]string, len(c))
	for i, d := range c {
		out[i] = d.Key
	}
	return out
}

func catalogByKey(c []FieldDescriptor) map[string]FieldDescriptor {
	m := make(map[string]FieldDescriptor, len(c))
	for _, d := range c {
		m[d.Key] = d
	}
	return m
}

func mustContain(t *testing.T, keys []string, key string) {
	t.Helper()
	for _, k := range keys {
		if k == key {
			return
		}
	}
	t.Errorf("catalog missing key %q; catalog: %v", key, keys)
}

func mustNotContain(t *testing.T, keys []string, key string) {
	t.Helper()
	for _, k := range keys {
		if k == key {
			t.Errorf("catalog should not contain key %q", key)
			return
		}
	}
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
