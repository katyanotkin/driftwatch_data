PYTHON  := .venv/bin/python
PIP     := .venv/bin/pip
PROJECT ?= $(shell grep GCP_PROJECT_DW .env 2>/dev/null | cut -d= -f2 | tr -d ' ')
REGION  ?= us-central1
ENV     ?= prod
IMAGE   ?= gcr.io/$(PROJECT)/driftwatch

# Job/scheduler names are env-scoped
JOB_DAILY   := driftwatch-daily-$(ENV)
JOB_PROFILE := driftwatch-profile-$(ENV)
SA          := driftwatch-sa@$(PROJECT).iam.gserviceaccount.com

.PHONY: help install lint test \
        run-daily run-profile add-note \
        gcp-setup gcp-billing gcp-apis gcp-sa bq-init \
        docker-build docker-push \
        deploy-daily deploy-profile \
        scheduler-daily scheduler-profile \
	backfill-local \
        deploy-all

help:
	@echo ""
	@echo "  DriftWatch — available targets"
	@echo ""
	@echo "  Dev"
	@echo "    install             Install Python dependencies into .venv."
	@echo "    lint                Run ruff linter"
	@echo "    test                Run pytest"
	@echo ""
	@echo "  Local runs  (ENV=stage|prod, default: prod)"
	@echo "    run-daily           OHLCV collection + event detection"
	@echo "    run-profile         4-week profile snapshot + event detection"
	@echo "    add-note            Add manager note  (SYMBOL= NOTE= [DATE=])"
	@echo ""
	@echo "  GCP setup  (run once per project)"
	@echo "    gcp-setup           Create project, link billing, enable APIs, create SA"
	@echo "    bq-init             Create BQ dataset + tables for ENV"
	@echo ""
	@echo "  Docker"
	@echo "    docker-build        Build container image"
	@echo "    docker-push         Push image to GCR"
	@echo ""
	@echo "  Cloud Run Jobs"
	@echo "    deploy-daily        Deploy/update $(JOB_DAILY)"
	@echo "    deploy-profile      Deploy/update $(JOB_PROFILE)"
	@echo ""
	@echo "  Cloud Scheduler"
	@echo "    scheduler-daily     Create/update daily scheduler trigger"
	@echo "    scheduler-profile   Create/update profile scheduler trigger"
	@echo ""
	@echo "  deploy-all            docker-build + push + deploy both jobs"
	@echo ""
	@echo "  Examples:"
	@echo "    make gcp-setup PROJECT=driftwatch-prod"
	@echo "    make bq-init ENV=stage"
	@echo "    make run-daily ENV=stage"
	@echo "    make deploy-all ENV=prod"
	@echo "    make add-note SYMBOL=SPY NOTE='rebalancing soon' ENV=prod"
	@echo "    backfill-local      Collect Backfill OHLCV data locally  (START_DATE=, END_DATE=)"
	@echo ""

# ---------------------------------------------------------------------------
# Dev
# ---------------------------------------------------------------------------

install:
	$(PIP) install -r requirements.txt

lint:
	$(PYTHON) -m ruff check driftwatch/ jobs/ tests/

test:
	$(PYTHON) -m pytest tests/ -v

# ---------------------------------------------------------------------------
# Local runs
# ---------------------------------------------------------------------------

run-daily:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) $(PYTHON) jobs/run_daily.py

run-profile:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) $(PYTHON) jobs/run_profile.py

# Usage: make add-note SYMBOL=SPY NOTE="rebalancing expected" DATE=2026-04-09
add-note:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) \
	  $(PYTHON) jobs/add_note.py $(SYMBOL) "$(NOTE)" $(if $(DATE),--date $(DATE),)

# Usage: make backfill-local
# Or:    make backfill-local START_DATE=2026-03-01 END_DATE=2026-04-09
backfill-local:
	PYTHONPATH=. \
	  $(if $(START_DATE),START_DATE=$(START_DATE),) \
	  $(if $(END_DATE),END_DATE=$(END_DATE),) \
	  $(PYTHON) jobs/backfill.py

# ---------------------------------------------------------------------------
# GCP setup (one-time per project)
# ---------------------------------------------------------------------------

gcp-setup: gcp-billing gcp-apis gcp-sa

gcp-billing:
	@echo ">>> Creating project $(PROJECT) ..."
	gcloud projects create $(PROJECT) --name="DriftWatch" || true
	@echo ">>> Detecting billing account ..."
	$(eval BILLING := $(shell gcloud billing accounts list \
	  --filter="open=true" --format="value(name)" --limit=1))
	@if [ -z "$(BILLING)" ]; then \
	  echo "ERROR: no open billing account found."; \
	  echo "       Link manually: https://console.cloud.google.com/billing/linkedaccount?project=$(PROJECT)"; \
	  exit 1; \
	fi
	@echo "    Using billing account: $(BILLING)"
	gcloud billing projects link $(PROJECT) --billing-account=$(BILLING) || \
	  (echo "Billing link failed — link manually at https://console.cloud.google.com/billing/linkedaccount?project=$(PROJECT)" && exit 1)

gcp-apis:
	@echo ">>> Enabling required APIs ..."
	gcloud services enable \
	  bigquery.googleapis.com \
	  run.googleapis.com \
	  cloudscheduler.googleapis.com \
	  secretmanager.googleapis.com \
	  cloudbuild.googleapis.com \
	  containerregistry.googleapis.com \
	  --project=$(PROJECT)

gcp-sa:
	@echo ">>> Creating service account driftwatch-sa ..."
	gcloud iam service-accounts create driftwatch-sa \
	  --display-name="DriftWatch Service Account" \
	  --project=$(PROJECT) || true

	@echo ">>> Granting IAM roles ..."
	gcloud projects add-iam-policy-binding $(PROJECT) \
	  --member="serviceAccount:$(SA)" \
	  --role="roles/bigquery.dataEditor" --quiet
	gcloud projects add-iam-policy-binding $(PROJECT) \
	  --member="serviceAccount:$(SA)" \
	  --role="roles/bigquery.jobUser" --quiet
	gcloud projects add-iam-policy-binding $(PROJECT) \
	  --member="serviceAccount:$(SA)" \
	  --role="roles/secretmanager.secretAccessor" --quiet
	gcloud projects add-iam-policy-binding $(PROJECT) \
	  --member="serviceAccount:$(SA)" \
	  --role="roles/run.invoker" --quiet

	@echo ">>> Storing ANTHROPIC_API_KEY in Secret Manager ..."
	@if ! gcloud secrets describe anthropic-api-key --project=$(PROJECT) > /dev/null 2>&1; then \
	  echo "$$ANTHROPIC_API_KEY" | gcloud secrets create anthropic-api-key \
	    --data-file=- --project=$(PROJECT); \
	else \
	  echo "    Secret already exists — skipping (update manually if key changed)"; \
	fi

	@echo ""
	@echo "GCP project $(PROJECT) is ready."
	@echo "Next: make bq-init ENV=prod && make bq-init ENV=stage"

# ---------------------------------------------------------------------------
# BQ tables
# ---------------------------------------------------------------------------

bq-init:
	@echo ">>> Initialising BQ tables for ENV=$(ENV) in project $(PROJECT) ..."
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) $(PYTHON) -c \
	  "from driftwatch.bq_client import bq_client; bq_client.ensure_tables(); \
	   print('BQ tables ready:', bq_client._dataset)"

# ---------------------------------------------------------------------------
# Docker
# ---------------------------------------------------------------------------

docker-build:
	docker build -f deploy/Dockerfile -t $(IMAGE):latest .

docker-push: docker-build
	docker push $(IMAGE):latest

# ---------------------------------------------------------------------------
# Cloud Run Jobs
# ---------------------------------------------------------------------------

_deploy-job:
	gcloud run jobs update $(JOB) \
	  --image=$(IMAGE):latest \
	  --region=$(REGION) \
	  --project=$(PROJECT) \
	  --command=python \
	  --args="$(CMD)" \
	  --set-secrets="ANTHROPIC_API_KEY=anthropic-api-key:latest" \
	  --set-env-vars="DW_ENV=$(ENV),GCP_PROJECT=$(PROJECT)" \
	  --service-account=$(SA) \
	  || gcloud run jobs create $(JOB) \
	  --image=$(IMAGE):latest \
	  --region=$(REGION) \
	  --project=$(PROJECT) \
	  --command=python \
	  --args="$(CMD)" \
	  --set-secrets="ANTHROPIC_API_KEY=anthropic-api-key:latest" \
	  --set-env-vars="DW_ENV=$(ENV),GCP_PROJECT=$(PROJECT)" \
	  --service-account=$(SA)

deploy-daily:
	$(MAKE) _deploy-job JOB=$(JOB_DAILY) CMD=jobs/run_daily.py

deploy-profile:
	$(MAKE) _deploy-job JOB=$(JOB_PROFILE) CMD=jobs/run_profile.py

# ---------------------------------------------------------------------------
# Cloud Scheduler
# ---------------------------------------------------------------------------

_scheduler-job:
	gcloud scheduler jobs create http $(TRIGGER) \
	  --schedule="$(SCHEDULE)" \
	  --uri="https://$(REGION)-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/$(PROJECT)/jobs/$(JOB):run" \
	  --http-method=POST \
	  --oauth-service-account-email=$(SA) \
	  --location=$(REGION) \
	  --time-zone="UTC" \
	  || gcloud scheduler jobs update http $(TRIGGER) \
	  --schedule="$(SCHEDULE)" \
	  --location=$(REGION)

scheduler-daily:
	$(MAKE) _scheduler-job \
	  TRIGGER=$(JOB_DAILY)-trigger \
	  JOB=$(JOB_DAILY) \
	  SCHEDULE="0 22 * * 1-5"

scheduler-profile:
	$(MAKE) _scheduler-job \
	  TRIGGER=$(JOB_PROFILE)-trigger \
	  JOB=$(JOB_PROFILE) \
	  SCHEDULE="0 23 * * 0"

# ---------------------------------------------------------------------------
# All-in-one deploy
# ---------------------------------------------------------------------------

deploy-all: docker-build docker-push deploy-daily deploy-profile
	@echo "Deployed ENV=$(ENV) jobs."
