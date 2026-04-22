{{/*
  ml-module.fullname
  Nom complet de la ressource.
  Format : {release-name}-{module.name}-{module.version}-{module.mode}
  Exemple : helm install preprocessing-supernovae → "preprocessing-supernovae-v1-streaming"
*/}}
{{- define "ml-module.fullname" -}}
{{- printf "%s-%s-%s-%s" .Release.Name .Values.module.name .Values.module.version .Values.module.mode | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
  ml-module.labels
  Labels standards appliqués à toutes les ressources K8s du chart.
  Helm recommande ces labels pour identifier et gérer les ressources.
*/}}
{{- define "ml-module.labels" -}}
{{ include "ml-module.selectorLabels" . }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
app.kubernetes.io/version: {{ .Values.module.version | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/component: {{ .Values.module.componentType }}
fink/module-name: {{ .Values.module.name }}
fink/module-mode: {{ .Values.module.mode }}
{{- end }}

{{/*
  ml-module.selectorLabels
  Labels utilisés par le Deployment pour identifier SES Pods.
  Doit être un sous-ensemble stable de labels (ne change jamais après le premier deploy).
*/}}
{{- define "ml-module.selectorLabels" -}}
app.kubernetes.io/name: ml-module
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
