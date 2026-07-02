{{- define "fyralis.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fyralis.fullname" -}}
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

{{- define "fyralis.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version | replace "+" "_" }}
app.kubernetes.io/name: {{ include "fyralis.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
fyralis.io/deployment-id: {{ .Values.global.deploymentId | quote }}
fyralis.io/customer-id: {{ .Values.global.customerId | quote }}
fyralis.io/environment: {{ .Values.global.environment | quote }}
{{- end -}}

{{- define "fyralis.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fyralis.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{- define "fyralis.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "fyralis.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{- define "fyralis.image" -}}
{{- printf "%s:%s" .Values.image.repository .Values.image.tag -}}
{{- end -}}

{{- define "fyralis.appEnvConfigName" -}}
{{- printf "%s-app-env" (include "fyralis.fullname" .) -}}
{{- end -}}

{{- define "fyralis.appSecretName" -}}
{{- printf "%s-app-secret" (include "fyralis.fullname" .) -}}
{{- end -}}

{{- define "fyralis.postgresName" -}}
{{- printf "%s-postgres" (include "fyralis.fullname" .) -}}
{{- end -}}

{{- define "fyralis.kafkaName" -}}
{{- printf "%s-kafka" (include "fyralis.fullname" .) -}}
{{- end -}}

{{- define "fyralis.minioName" -}}
{{- printf "%s-minio" (include "fyralis.fullname" .) -}}
{{- end -}}

{{- define "fyralis.redisName" -}}
{{- printf "%s-redis" (include "fyralis.fullname" .) -}}
{{- end -}}

{{- define "fyralis.waitForLocalServices" -}}
- name: wait-for-local-services
  image: {{ include "fyralis.image" . }}
  imagePullPolicy: {{ .Values.image.pullPolicy }}
  command:
    - /bin/sh
    - -ec
    - |
      until pg_isready -h {{ include "fyralis.postgresName" . }} -U {{ .Values.postgres.user }} -d {{ .Values.postgres.database }}; do sleep 2; done
      python - <<'PY'
      import socket
      import time
      targets = [
          ("{{ include "fyralis.kafkaName" . }}", 9092),
          ("{{ include "fyralis.minioName" . }}", 9000),
          ("{{ include "fyralis.redisName" . }}", 6379),
      ]
      for host, port in targets:
          for _ in range(120):
              try:
                  with socket.create_connection((host, port), timeout=2):
                      break
              except OSError:
                  time.sleep(2)
          else:
              raise SystemExit(f"{host}:{port} was not reachable")
      PY
{{- end -}}
