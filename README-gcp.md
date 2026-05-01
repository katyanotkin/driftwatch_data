# GCP Setup — sigforge

**Project:** `driftwatch2` (number `852907129360`)
**Dataset:** `sigforge_prod` / `sigforge_stage`
**Target account:** `katyanotkin@gmail.com`

> **Two accounts are configured on this machine.**
> Run the verification block below before every deploy step.
> Do not skip it.

---

## 0. Verify you are on the right account and project

```bash
gcloud auth list
# ACTIVE account must be: katyanotkin@gmail.com
# If it shows kate.middlesex@gmail.com as active, run step 0a first.

gcloud config get-value account   # must print: katyanotkin@gmail.com
gcloud config get-value project   # must print: driftwatch2
```

### 0a. Switch account (if needed)

```bash
gcloud config set account katyanotkin@gmail.com
```

### 0b. Set the project (if needed)

```bash
gcloud config set project driftwatch2
```

Confirm both together:

```bash
gcloud config list --format="value(core.account, core.project)"
# katyanotkin@gmail.com
# driftwatch2
```

---

## 1. Authenticate (first time, or if token expired)

```bash
gcloud auth login katyanotkin@gmail.com
gcloud auth application-default login
```

Application Default Credentials (ADC) are what the Python SDK uses.
If you skip `application-default login` the BigQuery client will fail locally.

---

## 2. Enable APIs (one-time per project)

```bash
gcloud services enable \
  bigquery.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  cloudbuild.googleapis.com \
  containerregistry.googleapis.com \
  --project=driftwatch2
```

---

## 3. Create service account (one-time)

```bash
gcloud iam service-accounts create sigforge-sa \
  --display-name="sigforge Service Account" \
  --project=driftwatch2

SA=sigforge-sa@driftwatch2.iam.gserviceaccount.com

gcloud projects add-iam-policy-binding driftwatch2 \
  --member="serviceAccount:$SA" --role="roles/bigquery.dataEditor" --quiet
gcloud projects add-iam-policy-binding driftwatch2 \
  --member="serviceAccount:$SA" --role="roles/bigquery.jobUser" --quiet
gcloud projects add-iam-policy-binding driftwatch2 \
  --member="serviceAccount:$SA" --role="roles/secretmanager.secretAccessor" --quiet
gcloud projects add-iam-policy-binding driftwatch2 \
  --member="serviceAccount:$SA" --role="roles/run.invoker" --quiet
```

Or use the Makefile shortcut (runs all of the above):

```bash
make gcp-sa PROJECT=driftwatch2
```

---

## 4. Store the Anthropic API key in Secret Manager (one-time)

```bash
echo -n "$ANTHROPIC_API_KEY" | gcloud secrets create anthropic-api-key \
  --data-file=- \
  --project=driftwatch2
```

To update the key later:

```bash
echo -n "$ANTHROPIC_API_KEY" | gcloud secrets versions add anthropic-api-key \
  --data-file=- \
  --project=driftwatch2
```

---

## 5. Create BigQuery tables

```bash
# Production dataset
make bq-init PROJECT=driftwatch2 ENV=prod

# Staging dataset (optional but recommended for testing)
make bq-init PROJECT=driftwatch2 ENV=stage
```

This creates `driftwatch2.sigforge_prod` and `driftwatch2.sigforge_stage`
with tables: `ticker_daily`, `ticker_profile`, `ticker_features`, `ticker_events`.

---

## 6. Local test run (no Docker)

```bash
# Verify .env is correct
cat .env
# GCP_PROJECT=driftwatch2
# DW_ENV=prod
# ANTHROPIC_API_KEY=<your key>

make run-daily  ENV=prod PROJECT=driftwatch2
```

---

## 7. Docker build and push

```bash
# Verify account before pushing
gcloud config get-value account   # must be katyanotkin@gmail.com
gcloud config get-value project   # must be driftwatch2

make docker-build PROJECT=driftwatch2
make docker-push  PROJECT=driftwatch2
# Image: gcr.io/driftwatch2/sigforge:latest
```

---

## 8. Deploy Cloud Run Jobs

```bash
make deploy-daily   PROJECT=driftwatch2 ENV=prod REGION=us-central1
make deploy-profile PROJECT=driftwatch2 ENV=prod REGION=us-central1
```

Or both at once:

```bash
make deploy-all PROJECT=driftwatch2 ENV=prod REGION=us-central1
```

---

## 9. Create Cloud Scheduler triggers

```bash
make scheduler-daily   PROJECT=driftwatch2 ENV=prod REGION=us-central1
# → runs weekdays at 22:00 UTC

make scheduler-profile PROJECT=driftwatch2 ENV=prod REGION=us-central1
# → runs Sundays at 23:00 UTC (~6-week cadence managed separately)
```

---

## Backfill

```bash
# Write to BigQuery
make backfill-local START=2025-01-01 END=2025-03-31 \
  PROJECT=driftwatch2 ENV=prod

# Inspect locally first (CSV, no BQ write)
make backfill-local START=2025-01-01 END=2025-01-31 \
  CSV=data/backfill_check.csv
```

---

## Quick-reference: safety checks

| What to check | Command |
|---|---|
| Active account | `gcloud config get-value account` |
| Active project | `gcloud config get-value project` |
| All credentialed accounts | `gcloud auth list` |
| ADC account | `gcloud auth application-default print-access-token \| head -1` |
| BQ dataset exists | `bq ls --project_id=driftwatch2` |
| Cloud Run jobs | `gcloud run jobs list --project=driftwatch2 --region=us-central1` |
