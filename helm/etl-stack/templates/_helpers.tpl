{{/*
Expand the name of the chart.
*/}}
{{- define "etl-stack.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "etl-stack.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
DNS suffix for in-cluster service references.
Usage: {{ include "etl-stack.svcFQDN" (dict "svc" "kafka" "ctx" .) }}
*/}}
{{- define "etl-stack.svcFQDN" -}}
{{ .svc }}.{{ .ctx.Release.Namespace }}.svc.cluster.local
{{- end }}
