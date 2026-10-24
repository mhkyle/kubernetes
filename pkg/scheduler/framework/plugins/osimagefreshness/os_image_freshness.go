/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package osimagefreshness implements a preference for newer node OS image versions.
package osimagefreshness

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/validation"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler/framework/plugins/names"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
)

const (
	// Name identifies the plugin in scheduler configuration.
	Name = names.OSImageFreshness
	// DefaultLabelKey is the node label containing version-<nonnegative integer>.
	DefaultLabelKey = "node.osimage"
	// A negative raw score distinguishes missing versions from valid version-0.
	unknownVersion int64 = -1
)

// OSImageFreshness ranks candidate nodes by their numeric OS image version.
type OSImageFreshness struct {
	labelKey string
}

var _ fwk.ScorePlugin = &OSImageFreshness{}
var _ fwk.ScoreExtensions = &OSImageFreshness{}

type pluginArgs struct {
	LabelKey string `json:"labelKey"`
}

// New initializes the plugin with an optional labelKey argument.
func New(_ context.Context, configuration runtime.Object, _ fwk.Handle) (fwk.Plugin, error) {
	args := pluginArgs{LabelKey: DefaultLabelKey}
	if err := frameworkruntime.DecodeInto(configuration, &args); err != nil {
		return nil, fmt.Errorf("decoding %s arguments: %w", Name, err)
	}
	if errs := validation.IsQualifiedName(args.LabelKey); len(errs) != 0 {
		return nil, fmt.Errorf("invalid labelKey %q: %s", args.LabelKey, strings.Join(errs, "; "))
	}
	return &OSImageFreshness{labelKey: args.LabelKey}, nil
}

// Name returns the plugin's registered name.
func (p *OSImageFreshness) Name() string {
	return Name
}

// Score returns an intermediate version number; NormalizeScore bounds the final score.
func (p *OSImageFreshness) Score(_ context.Context, _ fwk.CycleState, _ *v1.Pod, nodeInfo fwk.NodeInfo) (int64, *fwk.Status) {
	return parseVersion(nodeInfo.Node().Labels[p.labelKey]), nil
}

func parseVersion(value string) int64 {
	suffix, ok := strings.CutPrefix(value, "version-")
	if !ok || suffix == "" {
		return unknownVersion
	}
	for _, digit := range suffix {
		if digit < '0' || digit > '9' {
			return unknownVersion
		}
	}
	version, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil {
		return unknownVersion
	}
	return version
}

// ScoreExtensions returns the normalization implementation.
func (p *OSImageFreshness) ScoreExtensions() fwk.ScoreExtensions {
	return p
}

// NormalizeScore ranks distinct valid versions, reserving zero for unknown versions.
func (p *OSImageFreshness) NormalizeScore(_ context.Context, _ fwk.CycleState, _ *v1.Pod, scores fwk.NodeScoreList) *fwk.Status {
	versions := make([]int64, 0, len(scores))
	for _, score := range scores {
		if score.Score >= 0 {
			versions = append(versions, score.Score)
		}
	}
	slices.Sort(versions)
	versions = slices.Compact(versions)

	for i := range scores {
		if scores[i].Score < 0 {
			scores[i].Score = fwk.MinNodeScore
			continue
		}
		if len(versions) == 1 {
			scores[i].Score = fwk.MaxNodeScore
			continue
		}
		rank, _ := slices.BinarySearch(versions, scores[i].Score)
		scores[i].Score = 1 + (fwk.MaxNodeScore-1)*int64(rank)/int64(len(versions)-1)
	}
	return nil
}
