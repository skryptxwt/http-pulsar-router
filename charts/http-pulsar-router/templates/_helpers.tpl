{{- define "http-pulsar-router.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "http-pulsar-router.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "http-pulsar-router.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "http-pulsar-router.labels" -}}
helm.sh/chart: {{ include "http-pulsar-router.chart" . }}
{{ include "http-pulsar-router.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "http-pulsar-router.selectorLabels" -}}
app.kubernetes.io/name: {{ include "http-pulsar-router.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "http-pulsar-router.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "http-pulsar-router.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "http-pulsar-router.effectiveConfig" -}}
{{- $config := deepCopy .Values.config -}}
{{- $server := default dict $config.server -}}
{{- $auth := default dict $server.auth -}}
{{- if and (default false $auth.enabled) (empty $auth.bearerToken) (empty $auth.bearerTokenFile) -}}
{{- fail "config.server.auth.bearerToken or bearerTokenFile is required when config.server.auth.enabled=true" -}}
{{- end -}}
{{- if and .Values.pulsarAuth.enabled (empty $config.pulsar.authTokenFile) -}}
{{- $_ := set $config.pulsar "authTokenFile" (printf "%s/token" (trimSuffix "/" .Values.pulsarAuth.mountPath)) -}}
{{- end -}}
{{- $config | toPrettyJson -}}
{{- end -}}
