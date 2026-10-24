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

package main

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strconv"
	"strings"

	v1 "k8s.io/api/core/v1"
	extenderv1 "k8s.io/kube-scheduler/extender/v1"
)

func newHandler(labelKey string, maxRequestBytes int64) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, "ok\n")
	})
	mux.HandleFunc("POST /prioritize", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, maxRequestBytes)
		defer r.Body.Close()
		decoder := json.NewDecoder(r.Body)
		var args extenderv1.ExtenderArgs
		if err := decoder.Decode(&args); err != nil {
			requestError(w, err)
			return
		}
		// Only one JSON document is valid; also consume trailing bytes to enforce the size limit.
		if err := decoder.Decode(new(json.RawMessage)); err != io.EOF {
			requestError(w, err)
			return
		}
		if args.Pod == nil || args.Nodes == nil || args.NodeNames != nil {
			http.Error(w, "provide Pod and Nodes and omit NodeNames; configure nodeCacheCapable: false", http.StatusBadRequest)
			return
		}
		names := make(map[string]struct{}, len(args.Nodes.Items))
		for _, node := range args.Nodes.Items {
			if _, exists := names[node.Name]; exists || node.Name == "" {
				http.Error(w, "candidate node names must be nonempty and unique", http.StatusBadRequest)
				return
			}
			names[node.Name] = struct{}{}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(prioritize(args.Nodes.Items, labelKey))
	})
	return mux
}

func requestError(w http.ResponseWriter, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		http.Error(w, "request body exceeds --max-request-bytes", http.StatusRequestEntityTooLarge)
		return
	}
	http.Error(w, "request body must contain exactly one valid ExtenderArgs JSON object", http.StatusBadRequest)
}

func parseVersion(value string) int64 {
	suffix, ok := strings.CutPrefix(value, "version-")
	if !ok || suffix == "" {
		return -1
	}
	for _, digit := range suffix {
		if digit < '0' || digit > '9' {
			return -1
		}
	}
	version, err := strconv.ParseInt(suffix, 10, 64)
	if err != nil {
		return -1
	}
	return version
}

func prioritize(nodes []v1.Node, labelKey string) extenderv1.HostPriorityList {
	result := make(extenderv1.HostPriorityList, len(nodes))
	versions := make([]int64, 0, len(nodes))
	for i, node := range nodes {
		version := parseVersion(node.Labels[labelKey])
		result[i] = extenderv1.HostPriority{Host: node.Name, Score: version}
		if version >= 0 {
			versions = append(versions, version)
		}
	}
	slices.Sort(versions)
	versions = slices.Compact(versions)
	for i := range result {
		switch {
		case result[i].Score < 0:
			result[i].Score = extenderv1.MinExtenderPriority
		case len(versions) == 1:
			result[i].Score = extenderv1.MaxExtenderPriority
		default:
			// Rank rather than scale version numbers so adjacent or very large versions remain comparable.
			rank, _ := slices.BinarySearch(versions, result[i].Score)
			result[i].Score = 1 + (extenderv1.MaxExtenderPriority-1)*int64(rank)/int64(len(versions)-1)
		}
	}
	return result
}
