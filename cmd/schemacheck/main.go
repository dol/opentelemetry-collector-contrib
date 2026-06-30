// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Command schemacheck verifies that every $ref in the repository's
// config.schema.yaml files resolves, and can seed + generate the schemas for
// internal packages that are referenced but not yet generated.
//
// Background: schemagen (go.opentelemetry.io/collector/cmd/schemagen) emits a
// $ref such as "./internal/http.config" whenever a config field uses a type from
// an internal subpackage, but it does not generate the target schema, and
// `make generate-schemas` only visits directories that already contain a
// config.schema.yaml. A referenced internal package with no schema of its own is
// therefore invisible and its $ref dangles silently, breaking downstream schema
// consumers with no signal at build time.
//
// schemacheck reads schemas structurally (not via grep), classifies every $ref
// using the generator's own settings file (.schemagen.yaml: allowedRefs), and:
//
//	check (default) : reports ALL dangling refs and exits non-zero.
//	-seed           : seeds an empty config.schema.yaml in each missing internal
//	                  package and runs schemagen there, repeating to a fixpoint.
//
// Refs into internal/metadata are owned by mdatagen ("DO NOT EDIT"); schemacheck
// never seeds those — it reports that the component needs `make generate`.
package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

const schemaFile = "config.schema.yaml"

// schemagenPkg must match Makefile.Common's SCHEMAGEN_PKG so that schemas this
// tool generates are byte-identical to `make generate-schemas`. Overridable via
// the SCHEMAGEN_PKG environment variable.
const defaultSchemagenPkg = "go.opentelemetry.io/collector/cmd/schemagen@v0.22.1-0.20260615181954-d04d642d0a3e"

func main() {
	var (
		root   = flag.String("root", "", "repository root (defaults to git toplevel or cwd)")
		seed   = flag.Bool("seed", false, "seed and generate schemas for referenced internal packages, to a fixpoint")
		strict = flag.Bool("strict", false, "also fail on refs into mdatagen-owned packages (default: report as warnings)")
	)
	flag.Parse()

	r, err := resolveRoot(*root)
	if err != nil {
		fail(err)
	}

	c, err := newChecker(r)
	if err != nil {
		fail(err)
	}

	if *seed {
		if err := c.seed(); err != nil {
			fail(err)
		}
	}

	dangling, needsMdatagen, err := c.check()
	if err != nil {
		fail(err)
	}

	if len(dangling) == 0 && len(needsMdatagen) == 0 {
		fmt.Println("schemacheck: all config.schema.yaml $refs resolve.")
		return
	}

	// mdatagen-owned packages (those with a metadata.yaml or an internal/metadata
	// output dir) get their schema from `make generate`, not from schemagen
	// seeding. Report them, but only fail on them under -strict, so this guard
	// stays scoped to schemagen's responsibility by default.
	for _, d := range needsMdatagen {
		fmt.Printf("warning: %s -> %s is mdatagen-owned and unresolved (run `make generate` in the owning component)\n", d.from, d.ref)
	}
	for _, d := range dangling {
		fmt.Printf("dangling: %s -> %s (expected %s)\n", d.from, d.ref, d.expected)
	}

	failures := len(dangling)
	if *strict {
		failures += len(needsMdatagen)
	}
	if failures == 0 {
		fmt.Printf("\nschemacheck: %d mdatagen-owned ref(s) unresolved (warnings only; use -strict to fail).\n", len(needsMdatagen))
		return
	}
	fmt.Fprintf(os.Stderr, "\nFAIL: %d unresolved schema reference(s).\n", failures)
	if len(dangling) > 0 {
		fmt.Fprintln(os.Stderr, "Run `make generate-schemas` (which seeds internal packages) to populate the missing schemas.")
	}
	os.Exit(1)
}

type checker struct {
	root        string
	allowedRefs []string
	schemagen   string
}

func newChecker(root string) (*checker, error) {
	allowed, err := readAllowedRefs(filepath.Join(root, ".schemagen.yaml"))
	if err != nil {
		return nil, err
	}
	pkg := os.Getenv("SCHEMAGEN_PKG")
	if pkg == "" {
		pkg = defaultSchemagenPkg
	}
	return &checker{root: root, allowedRefs: allowed, schemagen: pkg}, nil
}

type unresolved struct {
	from     string // schema file (repo-relative) the ref appears in
	ref      string // the raw $ref value
	expected string // repo-relative path of the schema file that should back it
}

// check walks every schema and returns refs that resolve to no in-repo file,
// split into ones that are seedable (dangling) and ones owned by mdatagen.
func (c *checker) check() (dangling, needsMdatagen []unresolved, err error) {
	schemas, err := c.findSchemas()
	if err != nil {
		return nil, nil, err
	}
	for _, schema := range schemas {
		refs, rerr := refsIn(schema)
		if rerr != nil {
			return nil, nil, fmt.Errorf("read %s: %w", schema, rerr)
		}
		dir := filepath.Dir(schema)
		for _, ref := range refs {
			t := c.classify(ref, dir)
			if t.kind != refInRepo && t.kind != refMdatagen {
				continue
			}
			if _, statErr := os.Stat(t.targetFile); statErr == nil {
				continue
			}
			u := unresolved{
				from:     c.rel(schema),
				ref:      ref,
				expected: c.rel(t.targetFile),
			}
			if t.kind == refMdatagen {
				needsMdatagen = append(needsMdatagen, u)
			} else {
				dangling = append(dangling, u)
			}
		}
	}
	return dangling, needsMdatagen, nil
}

// seed seeds empty schemas for missing seedable internal packages and runs
// schemagen on them, repeating until no new packages need seeding (newly
// generated internal schemas may reference still-deeper internal packages).
func (c *checker) seed() error {
	for pass := 1; ; pass++ {
		dangling, _, err := c.check()
		if err != nil {
			return err
		}
		dirs := seedableDirs(dangling)
		if len(dirs) == 0 {
			if pass == 1 {
				fmt.Println("schemacheck: nothing to seed; all internal refs resolve.")
			}
			return nil
		}
		fmt.Printf("schemacheck: seed pass %d, generating %d internal package(s)\n", pass, len(dirs))
		for _, dir := range dirs {
			abs := filepath.Join(c.root, dir)
			if fi, statErr := os.Stat(abs); statErr != nil || !fi.IsDir() {
				fmt.Fprintf(os.Stderr, "  WARN: %s is not a directory (ref to a non-existent package); skipping\n", dir)
				continue
			}
			fmt.Printf("  seed+generate %s\n", dir)
			if err := os.WriteFile(filepath.Join(abs, schemaFile), []byte("$defs: {}\n"), 0o600); err != nil {
				return err
			}
			if err := c.runSchemagen(abs); err != nil {
				return fmt.Errorf("schemagen %s: %w", dir, err)
			}
		}
	}
}

func (c *checker) runSchemagen(dir string) error {
	cmd := exec.Command("go", "run", c.schemagen, dir, "-o", dir)
	cmd.Dir = c.root
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// seedableDirs returns the unique repo-relative directories that should be
// seeded for the given dangling refs (existing dirs only).
func seedableDirs(dangling []unresolved) []string {
	set := map[string]struct{}{}
	for _, d := range dangling {
		set[filepath.Dir(d.expected)] = struct{}{}
	}
	dirs := make([]string, 0, len(set))
	for d := range set {
		dirs = append(dirs, d)
	}
	sort.Strings(dirs)
	return dirs
}

type refKind int

const (
	refSameFile refKind = iota // bare $defs identifier (no dot)
	refExternal                // matches an allowedRefs prefix; resolved downstream
	refInRepo                  // backed by another package's config.schema.yaml
	refMdatagen                // backed by an internal/metadata schema (mdatagen)
)

type refTarget struct {
	kind       refKind
	targetFile string // absolute path, for refInRepo/refMdatagen
}

// classify maps a $ref to how it should resolve. dir is the directory of the
// schema the ref appears in (for "./"-relative refs).
func (c *checker) classify(ref, dir string) refTarget {
	// Bare $defs identifiers carry no dotted type suffix.
	if !strings.Contains(ref, ".") {
		return refTarget{kind: refSameFile}
	}
	// External module refs (e.g. go.opentelemetry.io/collector/...) are resolved
	// by downstream consumers, not by an in-repo file.
	for _, prefix := range c.allowedRefs {
		if ref == prefix || strings.HasPrefix(ref, prefix+"/") {
			return refTarget{kind: refExternal}
		}
	}
	// In-repo ref: "<pkg-path>.<snake_type>" where <pkg-path> is repo- or
	// schema-relative. Strip the trailing ".type" to get the package path.
	path := ref[:strings.LastIndex(ref, ".")]
	var targetDir string
	switch {
	case strings.HasPrefix(path, "/"):
		targetDir = filepath.Join(c.root, path)
	case strings.HasPrefix(path, "./"):
		targetDir = filepath.Join(dir, strings.TrimPrefix(path, "./"))
	default:
		targetDir = filepath.Join(c.root, path)
	}
	// A package is owned by mdatagen (and its schema comes from `make generate`,
	// not schemagen seeding) when it either IS an internal/metadata output
	// directory or contains a metadata.yaml of its own (a component package such
	// as a scraper or resource detector).
	kind := refInRepo
	if base := filepath.Base(targetDir); base == "metadata" && filepath.Base(filepath.Dir(targetDir)) == "internal" {
		kind = refMdatagen
	} else if _, err := os.Stat(filepath.Join(targetDir, "metadata.yaml")); err == nil {
		kind = refMdatagen
	}
	return refTarget{kind: kind, targetFile: filepath.Join(targetDir, schemaFile)}
}

func (c *checker) findSchemas() ([]string, error) {
	var out []string
	err := filepath.WalkDir(c.root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if d.Name() == schemaFile {
			out = append(out, path)
		}
		return nil
	})
	sort.Strings(out)
	return out, err
}

func (c *checker) rel(p string) string {
	r, err := filepath.Rel(c.root, p)
	if err != nil {
		return p
	}
	return r
}

// refsIn parses a schema file and returns every $ref string value found
// anywhere in the document.
func refsIn(file string) ([]string, error) {
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	var root yaml.Node
	if err := yaml.Unmarshal(data, &root); err != nil {
		return nil, err
	}
	var refs []string
	collectRefs(&root, &refs)
	return refs, nil
}

// collectRefs walks a YAML node tree, recording the scalar value of every
// mapping key named "$ref".
func collectRefs(n *yaml.Node, out *[]string) {
	switch n.Kind {
	case yaml.DocumentNode:
		for _, child := range n.Content {
			collectRefs(child, out)
		}
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			key, val := n.Content[i], n.Content[i+1]
			if key.Value == "$ref" && val.Kind == yaml.ScalarNode {
				*out = append(*out, val.Value)
			}
			collectRefs(val, out)
		}
	case yaml.SequenceNode:
		for _, child := range n.Content {
			collectRefs(child, out)
		}
	}
}

func readAllowedRefs(settingsFile string) ([]string, error) {
	data, err := os.ReadFile(settingsFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", settingsFile, err)
	}
	var s struct {
		AllowedRefs []string `yaml:"allowedRefs"`
	}
	if err := yaml.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", settingsFile, err)
	}
	return s.AllowedRefs, nil
}

func resolveRoot(flagRoot string) (string, error) {
	if flagRoot != "" {
		return filepath.Abs(flagRoot)
	}
	if out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output(); err == nil {
		return strings.TrimSpace(string(out)), nil
	}
	return os.Getwd()
}

func fail(err error) {
	if errors.Is(err, nil) {
		return
	}
	fmt.Fprintln(os.Stderr, "schemacheck:", err)
	os.Exit(2)
}
