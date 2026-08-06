"""Verify every Airflow/ETL metric queried by Grafana and the alert rules is
one the statsd-exporter mapping actually emits.

Grafana renders an unknown metric name as an empty panel, and Prometheus
treats an alert on an unknown name as permanently not-firing. Both fail
silently, so a typo here is invisible until someone notices a dashboard row
that has never drawn a line. That is exactly how the "Pipeline Orchestration"
row shipped querying airflow_ti_successes_total: statsd-exporter emits
airflow_ti_successes, with no _total suffix, under both the plain-text and
the OpenMetrics negotiation Prometheus uses.

Only airflow_*/etl_* names are checked — those are the ones the mapping owns.
kafka_*, minio_* and node_* come from their own exporters.

Run: python scripts/check_dashboard_metrics.py
"""

import glob
import json
import os
import re
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MAPPING = os.path.join(ROOT, 'monitoring', 'statsd_mapping.yml')
# Kubernetes gets the same mappings via the Airflow chart's statsd
# overrideMappings. If the two drift, the check below still passes while the
# dashboards go blank on the cluster only — the parity failure this repo keeps
# hitting — so compare them rather than trusting one file.
HELM_VALUES = os.path.join(ROOT, 'k8s', 'airflow', 'helm-values.yaml')

# Suffixes statsd-exporter genuinely appends when a mapping produces a
# summary/histogram (Airflow's timers), so alert rules may legitimately use
# them: airflow.dagrun.duration.failed.* -> airflow_dagrun_duration_failed_count.
DERIVED_SUFFIXES = ('_count', '_sum', '_bucket', '_created')

OWNED = re.compile(r'\b((?:airflow|etl)_[a-z0-9_]+)')


PAIR = re.compile(r'match:\s*["\']([^"\']+)["\'][\s\S]{0,80}?name:\s*["\']([A-Za-z0-9_]+)')


def mapping_pairs(path, after=None):
    with open(path, encoding='utf-8') as fh:
        text = fh.read()
    if after:
        text = text[text.find(after):]
    return set(PAIR.findall(text))


def emitted_names():
    with open(MAPPING, encoding='utf-8') as fh:
        return set(re.findall(r'^\s*name:\s*["\']?([A-Za-z0-9_]+)', fh.read(), re.M))


def mapping_drift():
    """statsd mappings that exist in one environment but not the other."""
    compose = mapping_pairs(MAPPING)
    k8s = mapping_pairs(HELM_VALUES, after='overrideMappings')
    return sorted(compose - k8s), sorted(k8s - compose)


def queried():
    """(metric, source-file) for every airflow_*/etl_* name we query."""
    found = []

    for path in glob.glob(os.path.join(ROOT, 'monitoring', 'grafana',
                                       'dashboards', '*.json')):
        with open(path, encoding='utf-8') as fh:
            dash = json.load(fh)
        for panel in dash.get('panels', []):
            for target in panel.get('targets', []):
                for name in OWNED.findall(target.get('expr', '')):
                    found.append((name, os.path.basename(path)))

    # Alert rules live inline in the compose config and again in the k8s
    # ConfigMap; both are plain `expr:` lines, so scan them as text rather
    # than taking a YAML dependency for two files.
    for rel in ('monitoring/alert_rules.yml', 'k8s/02-configmaps.yaml'):
        path = os.path.join(ROOT, rel)
        if not os.path.exists(path):
            continue
        with open(path, encoding='utf-8') as fh:
            for line in fh:
                if 'expr:' in line:
                    for name in OWNED.findall(line):
                        found.append((name, rel))

    return found


def resolves(name, names):
    if name in names:
        return True
    return any(name.endswith(s) and name[:-len(s)] in names
               for s in DERIVED_SUFFIXES)


def main():
    names = emitted_names()
    if not names:
        sys.exit(f"No mappings parsed from {MAPPING} — has the format changed?")

    bad = sorted({(m, src) for m, src in queried() if not resolves(m, names)})
    if bad:
        print("Metrics queried but never emitted by statsd_mapping.yml:\n")
        for metric, src in bad:
            print(f"  {metric}\n      queried in {src}")
            stem = metric
            for suffix in ('_total',) + DERIVED_SUFFIXES:
                if stem.endswith(suffix) and stem[:-len(suffix)] in names:
                    print(f"      did you mean {stem[:-len(suffix)]}?")
                    break
        print("\nstatsd-exporter emits mapping names verbatim; it does not add"
              " a _total suffix to counters.")
        sys.exit(1)

    only_compose, only_k8s = mapping_drift()
    if only_compose or only_k8s:
        print("statsd mappings differ between compose and Kubernetes, so the "
              "dashboards cannot match in both:\n")
        for match, name in only_compose:
            print(f"  only in monitoring/statsd_mapping.yml: {match} -> {name}")
        for match, name in only_k8s:
            print(f"  only in k8s/airflow/helm-values.yaml:  {match} -> {name}")
        sys.exit(1)

    print(f"OK - every airflow_*/etl_* metric queried resolves "
          f"({len(names)} mappings, compose and k8s agree).")


def _self_check():
    names = {'airflow_ti_successes', 'airflow_dagrun_duration_failed'}
    assert resolves('airflow_ti_successes', names)
    assert resolves('airflow_dagrun_duration_failed_count', names)  # timer
    assert not resolves('airflow_ti_successes_total', names)        # the bug
    assert not resolves('airflow_nope', names)


if __name__ == '__main__':
    _self_check()
    main()
