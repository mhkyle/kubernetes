# OS-version scheduler extender

A standalone HTTP Prioritize extender for the standard kube-scheduler. The
service compares node OS labels numerically without importing scheduler internals
or the in-process OSImageFreshness plugin. It does not require changes to the
kube-scheduler binary.

## Build and run

From the repository root:

```sh
go build -o /tmp/scheduler-extender ./cmd/scheduler-extender
/tmp/scheduler-extender --listen-address=127.0.0.1:8081 --label-key=node.osimage
```

Flags:

- `--listen-address`: defaults to `127.0.0.1:8081`.
- `--label-key`: defaults to `node.osimage`; must be a valid Kubernetes label key.
- `--max-request-bytes`: defaults to 33554432 (32 MiB). Increase it if full
  candidate NodeLists exceed this size.

Endpoints:

- `POST /prioritize`: accepts `k8s.io/kube-scheduler/extender/v1.ExtenderArgs`
  with `Pod` and `Nodes` and returns a `HostPriorityList`.
- `GET /healthz`: returns HTTP 200 while the HTTP server is available.

The service supports full NodeList requests only (`nodeCacheCapable: false`).
It needs no Kubernetes credentials, API calls, or node cache. NodeNames-only
requests, invalid JSON, duplicate/empty candidate names, and missing Pod/Nodes
receive HTTP 400. Oversized requests receive HTTP 413. An empty NodeList returns
`[]`. SIGINT and SIGTERM initiate graceful shutdown with a five-second deadline.

## Scheduler configuration

Run the service in the scheduler's network namespace (for example, as a sidecar)
when using the loopback address below. A different Pod's loopback address cannot
reach this service. The listener is plain HTTP; it does not configure TLS itself.

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
extenders:
  - urlPrefix: http://127.0.0.1:8081
    prioritizeVerb: prioritize
    weight: 3
    httpTimeout: 500ms
    nodeCacheCapable: false
    ignorable: true
```

Start kube-scheduler with `--config=<path>`. This configuration adds a preference
for every Pod handled by this scheduler; extenders are not profile-specific.
Keep filterVerb, bindVerb, and preemptVerb unset. Disable OSImageFreshness in the
participating profiles if it was enabled, to avoid counting the preference twice.

Prioritize failures and timeouts cause kube-scheduler to ignore this extender's
contribution for that attempt and continue with other scores. This remains a soft
preference, not a condition for scheduling.

## Scoring

Label values must match `version-<nonnegative decimal integer>` and fit in int64.
Leading zeros are accepted. Missing, empty, malformed, negative, or overflowing
values get 0. Valid version-0 is distinct from missing metadata.

Distinct valid versions among the supplied candidates are ranked numerically.
For multiple versions, rank r (starting at zero) receives:

```text
1 + floor(9 * r / (distinctVersionCount - 1))
```

If only one valid version exists, its nodes get 10. Equal versions tie, all-invalid
candidates all get 0, and more than ten distinct versions necessarily produce
some ties. For example:

| OS label | Extender score |
| --- | ---: |
| missing | 0 |
| version-1 | 1 |
| version-2 | 5 |
| version-10 | 10 |

Scores follow the extender protocol's 0–10 range, not the framework's 0–100
range. kube-scheduler scales and weights the contribution before adding it to
other scores: `extenderScore * weight * 10` in this checkout.

Only supplied feasible candidates are compared. Other weighted preferences can
outweigh OS freshness; existing Pods are not moved when labels change.

## Verify

```sh
go test -race ./cmd/scheduler-extender
```

This command currently lives in the Kubernetes repository for convenience. It can
be moved to an independent repository using the public k8s.io/api,
k8s.io/apimachinery, and k8s.io/kube-scheduler modules; no in-tree scoring-plugin
registration is needed.
