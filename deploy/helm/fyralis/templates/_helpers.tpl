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

{{/*
Validate the configuration surface for the customer-owned Figma OAuth app.

`app.extraEnv` is rendered into a ConfigMap, so keeping all OAuth material
there would make plaintext credentials visible to any ConfigMap reader. The
public settings below are safe in the ConfigMap. Managed-secret references are
loaded from `app.figmaOAuth.existingSecret` only by the gateway and the
workers that exchange or refresh Figma grants.
*/}}
{{- define "fyralis.validateFigmaOAuth" -}}
{{- $extraEnv := default (dict) .Values.app.extraEnv -}}
{{- $reservedKeys := list "FIGMA_OAUTH_ENABLED" "FIGMA_CLIENT_ID" "FIGMA_CLIENT_SECRET" "FIGMA_CLIENT_SECRET_SECRET_REF" "FIGMA_REDIRECT_URI" "FIGMA_OAUTH_UI_BASE_URL" "FIGMA_OAUTH_ALLOW_HTTP_LOOPBACK" "FIGMA_OAUTH_SCOPES" "OAUTH_STATE_HMAC_KEY" "OAUTH_STATE_HMAC_KEY_SECRET_REF" -}}
{{- range $key := $reservedKeys -}}
{{- if hasKey $extraEnv $key -}}
{{- fail (printf "app.extraEnv.%s is reserved for the safe Figma OAuth configuration; use app.figmaOAuth public settings and app.figmaOAuth.existingSecret" $key) -}}
{{- end -}}
{{- end -}}
{{- if .Values.app.figmaOAuth.enabled -}}
{{- if not .Values.app.figmaOAuth.clientId -}}
{{- fail "app.figmaOAuth.clientId is required when app.figmaOAuth.enabled=true" -}}
{{- end -}}
{{- if not .Values.app.figmaOAuth.redirectUri -}}
{{- fail "app.figmaOAuth.redirectUri is required when app.figmaOAuth.enabled=true" -}}
{{- end -}}
{{- if not .Values.app.figmaOAuth.uiBaseUrl -}}
{{- fail "app.figmaOAuth.uiBaseUrl is required when app.figmaOAuth.enabled=true" -}}
{{- end -}}
{{- if not .Values.app.figmaOAuth.scopes -}}
{{- fail "app.figmaOAuth.scopes is required when app.figmaOAuth.enabled=true" -}}
{{- end -}}
{{- if not .Values.app.figmaOAuth.existingSecret -}}
{{- fail "app.figmaOAuth.existingSecret is required when app.figmaOAuth.enabled=true; it must contain managed secret references, not plaintext OAuth secrets" -}}
{{- end -}}
{{- end -}}
{{- if not .Values.minio.bucket -}}
{{- fail "minio.bucket is required" -}}
{{- end -}}
{{- if not .Values.minio.blobBucket -}}
{{- fail "minio.blobBucket is required" -}}
{{- end -}}
{{- if eq .Values.minio.bucket .Values.minio.blobBucket -}}
{{- fail "minio.bucket and minio.blobBucket must be distinct" -}}
{{- end -}}
{{- end -}}

{{/*
Signal runtime identity is per Deployment, never global app configuration.
This prevents two tenant installations from inheriting the same identity or
accidentally contending on one process-global lease.
*/}}
{{- define "fyralis.validateSignalGateway" -}}
{{- $extraEnv := default (dict) .Values.app.extraEnv -}}
{{- $reservedKeys := list "SIGNAL_TENANT_ID" "SIGNAL_INSTALLATION_ID" "SIGNAL_JSONRPC_ENDPOINT" "SIGNAL_SSE_ENDPOINT" "SIGNAL_CLI_VERSION" "SIGNAL_CLI_MULTI_ACCOUNT" -}}
{{- range $key := $reservedKeys -}}
{{- if hasKey $extraEnv $key -}}
{{- fail (printf "app.extraEnv.%s is installation-scoped; configure it under signalGateway.installations instead" $key) -}}
{{- end -}}
{{- end -}}
{{- if .Values.signalGateway.enabled -}}
{{- if ne (toString .Values.signalGateway.signalCliVersion) "0.14.4.1" -}}
{{- fail "signalGateway.signalCliVersion must equal the certified version 0.14.4.1" -}}
{{- end -}}
{{- if eq (len .Values.signalGateway.installations) 0 -}}
{{- fail "signalGateway.installations must contain at least one exact binding when signalGateway.enabled=true" -}}
{{- end -}}
{{- $seenNames := dict -}}
{{- $seenBindings := dict -}}
{{- range $installation := .Values.signalGateway.installations -}}
{{- $name := toString $installation.name -}}
{{- $tenantID := toString $installation.tenantId -}}
{{- $installationID := toString $installation.installationId -}}
{{- $endpoint := toString $installation.jsonrpcEndpoint -}}
{{- $sseEndpoint := toString (default "" $installation.sseEndpoint) -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name) -}}
{{- fail (printf "signalGateway installation name %q must be a DNS label" $name) -}}
{{- end -}}
{{- if gt (len $name) 63 -}}
{{- fail (printf "signalGateway installation name %q must not exceed 63 characters" $name) -}}
{{- end -}}
{{- if hasKey $seenNames $name -}}
{{- fail (printf "duplicate signalGateway installation name %q" $name) -}}
{{- end -}}
{{- $recordedName := set $seenNames $name true -}}
{{- $uuidPattern := "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$" -}}
{{- if not (regexMatch $uuidPattern $tenantID) -}}
{{- fail (printf "signalGateway installation %q has an invalid tenantId UUID" $name) -}}
{{- end -}}
{{- if not (regexMatch $uuidPattern $installationID) -}}
{{- fail (printf "signalGateway installation %q has an invalid installationId UUID" $name) -}}
{{- end -}}
{{- if or (eq (lower $tenantID) "00000000-0000-0000-0000-000000000000") (eq (lower $installationID) "00000000-0000-0000-0000-000000000000") -}}
{{- fail (printf "signalGateway installation %q cannot use a nil UUID" $name) -}}
{{- end -}}
{{- $binding := printf "%s/%s" (lower $tenantID) (lower $installationID) -}}
{{- if hasKey $seenBindings $binding -}}
{{- fail (printf "duplicate signalGateway tenant/installation binding %q" $binding) -}}
{{- end -}}
{{- $recordedBinding := set $seenBindings $binding true -}}
{{- if not (regexMatch "^https?://.+" $endpoint) -}}
{{- fail (printf "signalGateway installation %q jsonrpcEndpoint must be HTTP(S)" $name) -}}
{{- end -}}
{{- if and $sseEndpoint (not (regexMatch "^https?://.+" $sseEndpoint)) -}}
{{- fail (printf "signalGateway installation %q sseEndpoint must be HTTP(S)" $name) -}}
{{- end -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Telegram runtime identity is per Deployment, never global app configuration.
Each exact tenant/installation pair owns its Telethon session and Redis lease.
*/}}
{{- define "fyralis.validateTelegramGateway" -}}
{{- $extraEnv := default (dict) .Values.app.extraEnv -}}
{{- $reservedKeys := list "TELEGRAM_TENANT_ID" "TELEGRAM_INSTALLATION_ID" -}}
{{- range $key := $reservedKeys -}}
{{- if hasKey $extraEnv $key -}}
{{- fail (printf "app.extraEnv.%s is installation-scoped; configure it under telegramGateway.installations instead" $key) -}}
{{- end -}}
{{- end -}}
{{- if .Values.telegramGateway.enabled -}}
{{- if eq (len .Values.telegramGateway.installations) 0 -}}
{{- fail "telegramGateway.installations must contain at least one exact binding when telegramGateway.enabled=true" -}}
{{- end -}}
{{- $seenNames := dict -}}
{{- $seenBindings := dict -}}
{{- range $installation := .Values.telegramGateway.installations -}}
{{- $name := toString $installation.name -}}
{{- $tenantID := toString $installation.tenantId -}}
{{- $installationID := toString $installation.installationId -}}
{{- if not (regexMatch "^[a-z0-9]([-a-z0-9]*[a-z0-9])?$" $name) -}}
{{- fail (printf "telegramGateway installation name %q must be a DNS label" $name) -}}
{{- end -}}
{{- if gt (len $name) 63 -}}
{{- fail (printf "telegramGateway installation name %q must not exceed 63 characters" $name) -}}
{{- end -}}
{{- if hasKey $seenNames $name -}}
{{- fail (printf "duplicate telegramGateway installation name %q" $name) -}}
{{- end -}}
{{- $_ := set $seenNames $name true -}}
{{- $uuidPattern := "^[0-9A-Fa-f]{8}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{4}-[0-9A-Fa-f]{12}$" -}}
{{- if not (regexMatch $uuidPattern $tenantID) -}}
{{- fail (printf "telegramGateway installation %q has an invalid tenantId UUID" $name) -}}
{{- end -}}
{{- if not (regexMatch $uuidPattern $installationID) -}}
{{- fail (printf "telegramGateway installation %q has an invalid installationId UUID" $name) -}}
{{- end -}}
{{- if or (eq (lower $tenantID) "00000000-0000-0000-0000-000000000000") (eq (lower $installationID) "00000000-0000-0000-0000-000000000000") -}}
{{- fail (printf "telegramGateway installation %q cannot use a nil UUID" $name) -}}
{{- end -}}
{{- $binding := printf "%s/%s" (lower $tenantID) (lower $installationID) -}}
{{- if hasKey $seenBindings $binding -}}
{{- fail (printf "duplicate telegramGateway tenant/installation binding %q" $binding) -}}
{{- end -}}
{{- $_ := set $seenBindings $binding true -}}
{{- end -}}
{{- end -}}
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
