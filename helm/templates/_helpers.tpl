{{/*
Expand the name of the chart.
*/}}
{{- define "parseable.name" -}}
{{- default .Chart.Name .Values.parseable.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "parseable.fullname" -}}
{{- if .Values.parseable.fullnameOverride }}
{{- .Values.parseable.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.parseable.nameOverride }}
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
{{- define "parseable.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "parseable.labels" -}}
helm.sh/chart: {{ include "parseable.chart" . }}
{{ include "parseable.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "parseable.selectorLabels" -}}
app.kubernetes.io/name: {{ include "parseable.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "parseable.serviceAccountName" -}}
{{- if .Values.parseable.serviceAccount.create }}
{{- default (include "parseable.fullname" .) .Values.parseable.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.parseable.serviceAccount.name }}
{{- end }}
{{- end }}
