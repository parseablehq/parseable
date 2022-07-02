{{/*
Expand the name of the chart.
*/}}
{{- define "collector.name" -}}
{{- default .Chart.Name .Values.collector.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "collector.fullname" -}}
{{- if .Values.collector.fullnameOverride }}
{{- .Values.collector.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.collector.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "collector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "collector.labels" -}}
helm.sh/chart: {{ include "collector.chart" . }}
{{ include "collector.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "collector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "collector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "collector.serviceAccountName" -}}
{{- if .Values.collector.serviceAccount.create }}
{{- default (include "collector.fullname" .) .Values.collector.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.collector.serviceAccount.name }}
{{- end }}
{{- end }}
