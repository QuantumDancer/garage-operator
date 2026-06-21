// Command downconvert rewrites Garage's OpenAPI 3.1.0 Admin API spec into the
// OpenAPI 3.0.x subset that oapi-codegen (via kin-openapi) can parse.
//
// oapi-codegen targets OpenAPI 3.0; the upstream spec is 3.1.0. The 3.1
// constructs Garage actually uses are:
//   - JSON-Schema-2020-12 nullable type arrays (`"type": ["string", "null"]`),
//   - nullable references expressed as `"oneOf": [{"type": "null"}, {"$ref": ...}]`
//     (3.0 forbids `type: "null"`), and
//   - a single boolean `items` schema.
//
// We rewrite each into its 3.0 equivalent so code generation stays reproducible
// with just the Go toolchain. Genuine multi-member oneOf unions (no null member)
// are left untouched — oapi-codegen handles those natively. If upstream starts
// using richer 3.1 features (const, numeric exclusiveMinimum, discriminators)
// this converter must grow.
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	if len(os.Args) != 3 {
		fmt.Fprintln(os.Stderr, "usage: downconvert <in-3.1.json> <out-3.0.json>")
		os.Exit(2)
	}

	raw, err := os.ReadFile(os.Args[1])
	if err != nil {
		fatal(err)
	}

	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		fatal(err)
	}

	doc["openapi"] = "3.0.3"
	convert(doc)

	out, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		fatal(err)
	}
	out = append(out, '\n')

	if err := os.WriteFile(os.Args[2], out, 0o644); err != nil {
		fatal(err)
	}
}

// convert walks the spec tree, rewriting every 3.1 schema construct in place.
func convert(node any) {
	switch n := node.(type) {
	case map[string]any:
		rewriteNullableType(n)
		rewriteNullableComposition(n)
		rewriteBooleanItems(n)
		for _, v := range n {
			convert(v)
		}
	case []any:
		for _, v := range n {
			convert(v)
		}
	}
}

// rewriteNullableType turns `"type": [T, "null"]` into `"type": T, "nullable": true`.
func rewriteNullableType(schema map[string]any) {
	types, ok := schema["type"].([]any)
	if !ok {
		return
	}

	var concrete []any
	nullable := false
	for _, t := range types {
		if t == "null" {
			nullable = true
			continue
		}
		concrete = append(concrete, t)
	}

	// Only the single-concrete-type + null shape is expressible in 3.0; anything
	// else (genuine multi-type unions) would need oneOf and is left untouched so
	// it fails loudly rather than silently mis-generating.
	if len(concrete) != 1 {
		return
	}
	schema["type"] = concrete[0]
	if nullable {
		schema["nullable"] = true
	}
}

// rewriteNullableComposition collapses the 3.1 nullable-reference idiom
// `oneOf/anyOf: [{"type": "null"}, S]` into a 3.0-compatible nullable schema.
// A single remaining `$ref` member becomes `allOf: [ref]` + `nullable: true`
// (a bare $ref cannot carry sibling keywords in 3.0); any other remaining set
// keeps the combinator minus the null member and gains `nullable: true`.
func rewriteNullableComposition(schema map[string]any) {
	for _, key := range []string{"oneOf", "anyOf"} {
		members, ok := schema[key].([]any)
		if !ok {
			continue
		}

		var kept []any
		hadNull := false
		for _, m := range members {
			if isNullSchema(m) {
				hadNull = true
				continue
			}
			kept = append(kept, m)
		}
		if !hadNull || len(kept) == 0 {
			continue
		}

		schema["nullable"] = true
		if len(kept) == 1 {
			if ref, isRef := kept[0].(map[string]any); isRef && ref["$ref"] != nil && len(ref) == 1 {
				delete(schema, key)
				schema["allOf"] = []any{ref}
				continue
			}
		}
		schema[key] = kept
	}
}

func isNullSchema(member any) bool {
	m, ok := member.(map[string]any)
	return ok && len(m) == 1 && m["type"] == "null"
}

// rewriteBooleanItems replaces a boolean `items` (JSON Schema 2020-12) with an
// empty schema, which 3.0 understands as "items of any type".
func rewriteBooleanItems(schema map[string]any) {
	if _, isBool := schema["items"].(bool); isBool {
		schema["items"] = map[string]any{}
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "downconvert:", err)
	os.Exit(1)
}
