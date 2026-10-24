# OSImageFreshness

An opt-in Score plugin that prefers nodes whose `node.osimage` label contains a
larger numeric version in the format `version-<int>`. It is registered in this
checkout's kube-scheduler binary but is not enabled by default.

## Configuration

```yaml
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
profiles:
  - schedulerName: os-aware-scheduler
    plugins:
      score:
        enabled:
          - name: OSImageFreshness
            weight: 3
    pluginConfig:
      - name: OSImageFreshness
        args:
          labelKey: node.osimage
```

Set `spec.schedulerName: os-aware-scheduler` on participating Pods and run the
built scheduler with `--config=<path>`. The optional `labelKey` defaults to
`node.osimage` and must be a valid Kubernetes label key.

## Scoring policy

- Versions are nonnegative decimal integers up to 9223372036854775807.
  Leading zeros are accepted and do not change the numeric version.
- Missing labels, empty values, malformed values, and overflowing integers get
  final score 0. They do not reject a node or fail the scheduling attempt.
- Distinct valid versions among the scored candidates are sorted numerically.
  With multiple versions, rank `r` (starting at zero) receives
  `1 + floor(99 * r / (distinctVersionCount - 1))`.
- If only one distinct valid version exists, every node with that version gets
  100. Equal versions always tie. If all versions are unknown, all scores are 0.
- For example, missing, `version-1`, `version-2`, and `version-10` receive 0, 1,
  50, and 100 respectively. More than 100 distinct valid versions necessarily
  produce some ties.

The plugin uses -1 internally for unknown versions so valid `version-0` and the
largest supported version can both be represented without overflow. Normalization
converts these intermediate values to the framework's 0–100 range before weighting.

This is a soft preference. Other plugins' weighted scores can outweigh it, and
hard filters still apply. Only the candidates presented to scoring are compared;
the scheduler may not examine every cluster node. Label changes affect subsequent
attempts after observation and do not migrate existing Pods.

The plugin deliberately does not implement SignPlugin, so its scores are not
reused through opportunistic batching. It has no mutable per-cycle shared state
and makes no API calls during scoring.
