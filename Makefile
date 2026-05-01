PYTHON  := .venv/bin/python
PIP     := .venv/bin/pip
PROJECT ?= $(shell grep GCP_PROJECT .env 2>/dev/null | cut -d= -f2 | tr -d ' ')
REGION  ?= us-central1
ENV     ?= prod
IMAGE   ?= gcr.io/$(PROJECT)/sigforge

JOB_DAILY   := sigforge-daily-$(ENV)
JOB_PROFILE := sigforge-profile-$(ENV)
SA          := sigforge-sa@$(PROJECT).iam.gserviceaccount.com

.PHONY: help install lint test \
        run-daily run-profile add-note backfill-local \
        gcp-setup gcp-billing gcp-apis gcp-sa bq-init \
        docker-build docker-push \
        deploy-daily deploy-profile \
        scheduler-daily scheduler-profile \
        deploy-all

help:
	@echo ""
	@echo "  sigforge — available targets"
	@echo ""
	@echo "  Dev"
	@echo "    install             Install Python dependencies into .venv"
	@echo "    lint                Run ruff linter"
	@echo "    test                Run pytest"
	@echo ""
	@echo "  Local runs  (ENV=stage|prod, default: prod)"
	@echo "    run-daily           OHLCV + features for today"
	@echo "    run-profile         Profile snapshot + GICS reclassification detection"
	@echo "    backfill-local      Backfill OHLCV + features (START=, END=, CSV=)"
	@echo "    add-note            Add manager note  (SYMBOL= NOTE= [DATE=])"
	@echo ""
	@echo "  GCP setup  (run once per project)"
	@echo "    gcp-setup           Create SA, enable APIs, store secrets"
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
	@echo "    scheduler-daily     Create/update daily trigger (weekdays 22:00 UTC)"
	@echo "    scheduler-profile   Create/update profile trigger (every 6 weeks)"
	@echo ""
	@echo "  deploy-all            docker-build + push + deploy both jobs"
	@echo ""

# ---------------------------------------------------------------------------
# Dev
# ---------------------------------------------------------------------------

install:
	$(PIP) install -r requirements.txt

lint:
	$(PYTHON) -m ruff check sigforge/ jobs/ tests/

test:
	$(PYTHON) -m pytest tests/ -v

# ---------------------------------------------------------------------------
# Local runs
# ---------------------------------------------------------------------------

run-daily:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) $(PYTHON) jobs/run_daily.py

run-profile:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) $(PYTHON) jobs/run_profile.py

# Usage: make backfill-local START=2025-01-01 END=2025-03-31
# CSV mode: make backfill-local START=2025-01-01 END=2025-01-31 CSV=data/backfill.csv
backfill-local:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) \
	  $(PYTHON) jobs/backfill.py \
	  $(if $(START),--start $(START),) \
	  $(if $(END),--end $(END),) \
	  $(if $(CSV),--out-csv $(CSV),)

# Usage: make add-note SYMBOL=AAPL NOTE="earnings beat expected" DATE=2025-04-30
add-note:
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) \
	  $(PYTHON) jobs/add_note.py $(SYMBOL) "$(NOTE)" $(if $(DATE),--date $(DATE),)

# ---------------------------------------------------------------------------
# GCP setup (one-time per project)
# ---------------------------------------------------------------------------

gcp-setup: gcp-billing gcp-apis gcp-sa

gcp-billing:
	@echo ">>> Creating project $(PROJECT) ..."
	gcloud projects create $(PROJECT) --name="sigforge" || true
	@echo ">>> Detecting billing account ..."
	$(eval BILLING := $(shell gcloud billing accounts list \
	  --filter="open=true" --format="value(name)" --limit=1))
	@if [ -z "$(BILLING)" ]; then \
	  echo "ERROR: no open billing account found."; exit 1; \
	fi
	gcloud billing projects link $(PROJECT) --billing-account=$(BILLING) || true

gcp-apis:
	gcloud services enable \
	  bigquery.googleapis.com \
	  run.googleapis.com \
	  cloudscheduler.googleapis.com \
	  secretmanager.googleapis.com \
	  cloudbuild.googleapis.com \
	  containerregistry.googleapis.com \
	  --project=$(PROJECT)

gcp-sa:
	gcloud iam service-accounts create sigforge-sa \
	  --display-name="sigforge Service Account" \
	  --project=$(PROJECT) || true

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

	@if ! gcloud secrets describe anthropic-api-key --project=$(PROJECT) > /dev/null 2>&1; then \
	  echo "$$ANTHROPIC_API_KEY" | gcloud secrets create anthropic-api-key \
	    --data-file=- --project=$(PROJECT); \
	else \
	  echo "    Secret already exists — skipping"; \
	fi

# ---------------------------------------------------------------------------
# BQ tables
# ---------------------------------------------------------------------------

bq-init:
	@echo ">>> Initialising BQ tables for ENV=$(ENV) in project $(PROJECT) ..."
	PYTHONPATH=. DW_ENV=$(ENV) GCP_PROJECT=$(PROJECT) $(PYTHON) -c \
	  "from sigforge.bq_client import BQClient; c = BQClient(); c.ensure_tables(); \
	   print('BQ tables ready:', c.dataset_ref)"

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

# Every 6 weeks on Sunday at 23:00 UTC (schedule approximated as "0 23 * * 0/6")
# Adjust or replace with Cloud Scheduler one-time jobs if exact 6-week cadence is required.
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
