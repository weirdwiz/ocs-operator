package rules

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/prometheus/prometheus/promql/parser"
	"gopkg.in/yaml.v3"
)

type prometheusRule struct {
	Spec struct {
		Groups []ruleGroup `yaml:"groups"`
	} `yaml:"spec"`
}

type ruleGroup struct {
	Name  string `yaml:"name"`
	Rules []rule `yaml:"rules"`
}

type rule struct {
	Record string `yaml:"record"`
	Alert  string `yaml:"alert"`
	Expr   string `yaml:"expr"`
}

func deployDir() string {
	_, thisFile, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(thisFile), "..", "..", "deploy")
}

func TestPromQLExpressionsAreValid(t *testing.T) {
	dir := deployDir()
	ruleFiles, err := filepath.Glob(filepath.Join(dir, "prometheus-ocs-rules*.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(ruleFiles) == 0 {
		t.Fatalf("no rule files found in %s", dir)
	}

	p := parser.NewParser(parser.Options{})

	for _, path := range ruleFiles {
		t.Run(filepath.Base(path), func(t *testing.T) {
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("reading %s: %v", path, err)
			}

			var pr prometheusRule
			if err := yaml.Unmarshal(data, &pr); err != nil {
				t.Fatalf("parsing %s: %v", path, err)
			}

			for _, group := range pr.Spec.Groups {
				for _, r := range group.Rules {
					name := r.Record
					if name == "" {
						name = r.Alert
					}
					expr := strings.TrimSpace(r.Expr)
					if expr == "" {
						t.Errorf("group %q rule %q: empty expression", group.Name, name)
						continue
					}

					if _, err := p.ParseExpr(expr); err != nil {
						t.Errorf("group %q rule %q: invalid PromQL: %v\nexpr: %s",
							group.Name, name, err, expr)
					}
				}
			}
		})
	}
}
