// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func writeSchema(t *testing.T, dir, content string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, schemaFile), []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func newTestChecker(t *testing.T, root string, allowed ...string) *checker {
	t.Helper()
	return &checker{root: root, allowedRefs: allowed, schemagen: "unused"}
}

func TestClassify(t *testing.T) {
	root := t.TempDir()
	// an mdatagen-owned package: has a metadata.yaml
	mdataDir := filepath.Join(root, "receiver/foo/internal/scraper/bar")
	if err := os.MkdirAll(mdataDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mdataDir, "metadata.yaml"), []byte("type: bar\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	c := newTestChecker(t, root, "go.opentelemetry.io/collector")
	cases := []struct {
		name string
		ref  string
		dir  string
		want refKind
	}{
		{"same-file defs id", "some_config", root, refSameFile},
		{"external allowed", "go.opentelemetry.io/collector/config/configopaque.string", root, refExternal},
		{"in-repo helper", "./internal/http.config", filepath.Join(root, "extension/x"), refInRepo},
		{"internal metadata", "./internal/metadata.metrics_config", filepath.Join(root, "receiver/y"), refMdatagen},
		{"package with metadata.yaml", "./internal/scraper/bar.config", filepath.Join(root, "receiver/foo"), refMdatagen},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := c.classify(tc.ref, tc.dir).kind; got != tc.want {
				t.Fatalf("classify(%q) kind = %d, want %d", tc.ref, got, tc.want)
			}
		})
	}
}

func TestCheckReportsDanglingAndMdatagen(t *testing.T) {
	root := t.TempDir()
	// A source schema referencing a missing helper package (dangling) and a
	// missing mdatagen-owned package.
	writeSchema(t, filepath.Join(root, "extension/x"), `
$defs:
  config:
    properties:
      a:
        $ref: ./internal/helper.config
      b:
        $ref: ./internal/metadata.resource_attributes_config
`)
	c := newTestChecker(t, root)

	dangling, needsMdatagen, err := c.check()
	if err != nil {
		t.Fatal(err)
	}
	if len(dangling) != 1 {
		t.Fatalf("dangling = %d, want 1 (%+v)", len(dangling), dangling)
	}
	if len(needsMdatagen) != 1 {
		t.Fatalf("needsMdatagen = %d, want 1 (%+v)", len(needsMdatagen), needsMdatagen)
	}
	if want := filepath.Join("extension/x/internal/helper", schemaFile); dangling[0].expected != want {
		t.Fatalf("dangling expected = %q, want %q", dangling[0].expected, want)
	}
}

func TestCheckResolvesWhenTargetExists(t *testing.T) {
	root := t.TempDir()
	writeSchema(t, filepath.Join(root, "extension/x"), `
$defs:
  config:
    properties:
      a:
        $ref: ./internal/helper.config
`)
	writeSchema(t, filepath.Join(root, "extension/x/internal/helper"), "$defs: {}\n")

	c := newTestChecker(t, root)
	dangling, needsMdatagen, err := c.check()
	if err != nil {
		t.Fatal(err)
	}
	if len(dangling) != 0 || len(needsMdatagen) != 0 {
		t.Fatalf("expected all resolved, got dangling=%v mdatagen=%v", dangling, needsMdatagen)
	}
}

func TestRefsIn(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "x")
	writeSchema(t, dir, `
$defs:
  a:
    $ref: ./internal/one.config
  b:
    properties:
      nested:
        $ref: ./internal/two.config
    allOf:
      - $ref: ./internal/three.config
`)
	refs, err := refsIn(filepath.Join(dir, schemaFile))
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 3 {
		t.Fatalf("refsIn found %d refs, want 3: %v", len(refs), refs)
	}
}
