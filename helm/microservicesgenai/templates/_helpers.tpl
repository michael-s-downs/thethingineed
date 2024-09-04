{{/* vim: set filetype=mustache: */}}
{{/*Expand the name of the chart.*/}}
{{- define "name" -}}
{{- default .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).*/}}
{{- define "fullname" -}}
{{- $name := default .Chart.Name -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*Create a default shorty qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).*/}}
{{- define "shortname" -}}
{{- $name := default .Chart.Name -}}
{{- printf "%s" $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*Create labels of services.*/}}
{{- define "labels" -}}
app: {{ template "shortname" . }}
chart: "{{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}"
release: {{ .Release.Name }}
heritage: {{ .Release.Service }}
{{- end }}

{{/*Create variables common in configmap.*/}}
{{- define "common" -}}
{{- $service := .Values.isService -}}
{{- range $key, $value := .Values.common }}
{{- if $value }}
{{- if and ($service) (eq $key "queue_delete_on_read") }}
{{- /*Not print nothing*/}}
{{- else }}
{{ $key | upper }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for queue.*/}}
{{- define "queues" -}}
{{- $queues := .Values.queues | default dict -}}
{{- $namespace := .Values.namespace -}}
{{- range $key := $queues }}
{{- if $key }}
{{ replace "-" "_" (printf "q_%s" $key) | upper }} : {{ printf "%s--q-%s" $namespace $key }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for redis.*/}}
{{- define "redis" -}}
{{- $redis := .Values.redis | default dict -}}
{{- range $key, $value := $redis }}
{{- if not (quote $value | empty) }}
{{ (printf "redis_db_%s" $key) | upper}}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for integration.*/}}
{{- define "integration" -}}
{{- $integration := .Values.integration | default dict -}}
{{- $namespace := .Values.namespace -}}
{{- range $key,$value := $integration }}
{{- if $value }}
{{- if eq $value "integration-sender" }}
{{ "integration_queue_url" | upper }}: {{ printf "%s--q-%s" $namespace $value }}
{{- else }}
{{ $key | upper }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for ocr.*/}}
{{- define "ocr" -}}
{{- $ocr := .Values.ocr | default dict -}}
{{- range $key,$value := $ocr }}
{{- if $value }}
{{ $key | upper }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for sender.*/}}
{{- define "sender" -}}
{{- $sender := .Values.sender | default dict -}}
{{- range $key,$value := $sender }}
{{- if $value }}
{{ $key | upper }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for compose.*/}}
{{- define "compose" -}}
{{- $compose := .Values.compose | default dict -}}
{{- range $key,$value := $compose }}
{{- if $value }}
{{ $key | upper }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for indexing.*/}}
{{- define "indexing" -}}
{{- $indexing := .Values.indexing | default dict -}}
{{- range $key,$value := $indexing }}
{{- if $value }}
{{ $key | upper }}: {{ $value }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for checktimeout.*/}}
{{- define "checktimeout" -}}
{{- $checktimeout := .Values.checktimeout | default dict -}}
{{- range $key,$value := $checktimeout }}
{{- if $value }}
{{ $key | upper }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for retrieve.*/}}
{{- define "retrieve" -}}
{{- $retrieve := .Values.retrieve | default dict -}}
{{- range $key,$value := $retrieve }}
{{- if $value }}
{{ $key | upper }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for gunicorn.*/}}
{{- define "gunicorn" -}}
{{- $gunicorn := .Values.gunicorn | default dict -}}
{{- $service :=  .Values.isService }}
{{- range $key,$value := $gunicorn }}
{{- if and ($value) ($service) }}
{{ $key | upper }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*Create variables for langfuse.*/}}
{{- define "langfuse" -}}
{{- $langfuse := .Values.langfuse | default dict -}}
{{- $addlangfuse :=  .Values.add_langfuse }}
{{- range $key,$value := $langfuse }}
{{- if and ($value) ($addlangfuse) }}
{{ $key | upper }}: {{ $value | quote }}
{{- end }}
{{- end }}
{{- end }}

{{/*Generate the imagePullSecret for a private Container Registry.*/}}
{{- define "imagePullSecret" }}
{{- with .Values.image }}
{{- printf "{\"auths\": {\"%s\": {\"username\": \"%s\", \"password\": \"%s\"}}}" .repository .username .password | b64enc }}
{{- end }}
{{- end }}

{{/*Create secrets with credentials.*/}}
{{- define "secrets" }}
{{- $secrets := .Values.secrets }}
{{- $name_secrets :=  .Values.name_secrets }}
{{- $namespace := .Values.namespace }}
{{- range $key,$value := $name_secrets }}
{{- $creds := index $secrets (printf "%s_creds" $value) }}
---
apiVersion: v1
kind: Secret
metadata:
  name: {{ $value }}-credentials
  namespace: {{ $namespace }}
data:
{{ $value }}.json: >-
{{ $creds | indent 2 }}
type: Opaque
{{- end }}
{{- end }}