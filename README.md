# mission-deh-hof-lyft-pipeline-codebase

Event-driven data pipeline that ingests NYC TLC **High Volume For-Hire Vehicle (HVFHV)**
trip data, filters it down to **Lyft (license `HV0005`)**, transforms it through a
raw → curated → aggregated flow, and publishes analytics-ready aggregate tables to S3.

The pipeline is orchestrated by AWS Step Functions and kicks off automatically when
a file lands in S3 (via an S3 event → Lambda → Step Functions trigger). All
infrastructure is captured as a CloudFormation template.

---

## Table of Contents

- [Architecture](#architecture)
- [Data Flow](#data-flow)
- [Repository Layout](#repository-layout)
- [Components](#components)
  - [Glue Jobs](#glue-jobs)
  - [Lambda Trigger](#lambda-trigger)
  - [Step Functions State Machine](#step-functions-state-machine)
  - [SNS Alerts](#sns-alerts)
- [S3 Data Layout](#s3-data-layout)
- [Audit Logging](#audit-logging)
- [Infrastructure (CloudFormation)](#infrastructure-cloudformation)
- [Deployment](#deployment)
- [Branching & Contribution Workflow](#branching--contribution-workflow)
- [Design Notes & Known Considerations](#design-notes--known-considerations)

---

## Architecture

```
                         (1) file lands
   External / upstream ─────────────────►  S3 bucket
                                             │
                                             │ (2) S3 event notification
                                             ▼
                                    Lambda: pipeline-trigger
                                             │
                                             │ (3) StartExecution
                                             ▼
                             Step Functions: mission-deh-hof-lyft-pipeline
                                             │
             ┌───────────────────────────────┼───────────────────────────────┐
             ▼                                ▼                                ▼
   (4) Glue: nyctlc-ingestion   (5) Glue: raw-to-curated-etl   (6) Glue: curated-to-aggregated-etl
     raw layer download            raw → curated (Spark)          curated → aggregated (Spark)
             │                                │                                │
             └───────────────┬────────────────┴────────────────┬──────────────┘
                             ▼                                  ▼
                     DynamoDB audit table                SNS success/failure alerts
```

Each Glue step runs synchronously (`glue:startJobRun.sync`), so the state machine
waits for a job to finish before starting the next. Any failure short-circuits to a
failure notification.

---

## Data Flow

| Stage | Job | Engine | Input | Output |
|-------|-----|--------|-------|--------|
| 1. Ingestion | `mission-deh-hof-nyctlc-ingestion` | Glue Python Shell | NYC TLC CloudFront (last 6 available months of HVFHV parquet + taxi zone lookup) | `raw/` layer + `raw/nyctlc/ingestion.done` marker |
| 2. Raw → Curated | `mission-deh-hof-raw-to-curated-etl` | Glue Spark | `raw/` HVFHV data | `curated/` cleaned & enriched Lyft trips (partitioned by `pickup_date`) |
| 3. Curated → Aggregated | `mission-deh-hof-curated-to-aggregated-etl` | Glue Spark | `curated/` Lyft trips | `aggregated/` 3 summary tables |

### Stage 2 details (raw → curated)
- Filters to Lyft only (`hvfhs_license_num == "HV0005"`).
- Applies data-quality rules (rejects bad `trip_miles`, `base_passenger_fare`,
  `trip_time`, null/invalid timestamps and location IDs). Rejected rows are written
  to `rejected/` with a `rejection_reason`.
- Adds derived columns: `trip_duration_minutes`, `pickup_date`, `pickup_hour`,
  `pickup_day_of_week`, `is_weekend`, `speed_mph`, `is_shared_ride`,
  `wait_time_minutes`.
- Filters unrealistic speeds (> 80 mph) and durations (< 1 min or > 240 min).
- Joins pickup and dropoff `LocationID` against the taxi zone lookup to add
  borough / zone / service-zone columns.

### Stage 3 details (curated → aggregated)
Produces three aggregate tables:
1. **`daily_borough_summary`** — trips, revenue, avg fare/tip/miles/duration/speed per day & borough (partitioned by `pickup_date`).
2. **`hourly_pattern_summary`** — trips, avg fare/miles/tip%/speed/wait by hour, weekend flag, and borough.
3. **`route_summary`** — trips, avg fare/miles/duration per pickup→dropoff zone pair (only routes with ≥ 10 trips).

---

## Repository Layout

```
.
├── README.md
├── .gitignore
├── cloudformation/
│   └── lyft-pipeline.yaml          # Full IaC: SNS, 3 Glue jobs, Step Function, Lambda
├── glue-scripts/
│   ├── backup-nyctlc-ingestion.py       # Stage 1: raw ingestion (Python Shell)
│   ├── backup-raw-to-curated.py         # Stage 2: raw → curated (Spark)
│   └── backup-curated-to-aggregated.py  # Stage 3: curated → aggregated (Spark)
├── lambda-scripts/
│   └── pipeline-trigger/
│       └── lambda_function.py      # S3-triggered Step Functions starter
└── step-functions/
    └── pipeline-state-machine.asl.json  # Reference ASL definition of the pipeline
```

> The `glue-scripts/` files are prefixed `backup-` to match the script keys used
> in the deployed environment (uploaded to `s3://<data-bucket>/scripts/`). The
> CloudFormation template points each Glue job at its corresponding script key.

---

## Components

### Glue Jobs

| Job | Type | Glue Version | Capacity | Timeout | Script |
|-----|------|--------------|----------|---------|--------|
| `mission-deh-hof-nyctlc-ingestion` | `pythonshell` (Python 3.9) | 3.0 | MaxCapacity 0.0625 | 30 min | `scripts/backup-nyctlc-ingestion.py` |
| `mission-deh-hof-raw-to-curated-etl` | `glueetl` (Spark, Python 3) | 5.1 | G.1X × 2 workers | 30 min | `scripts/backup-raw-to-curated.py` |
| `mission-deh-hof-curated-to-aggregated-etl` | `glueetl` (Spark, Python 3) | 5.1 | G.1X × 2 workers | 30 min | `scripts/backup-curated-to-aggregated.py` |

All three run with `MaxConcurrentRuns: 1` and auto-detect the target bucket from the
account ID (`mission-deh-hof-nyctlc-live-<ACCOUNT_ID>`).

### Lambda Trigger

- **Function:** `mission-deh-hof-pipeline-trigger`
- **Runtime:** Python 3.12, 128 MB, 30 s timeout, x86_64
- **Behavior:** On an S3 event, logs the incoming object key(s), then calls
  `StepFunctions.start_execution` with input
  `{"trigger": "s3_event", "source_file": <key>}`.
- **Config:** reads the target state machine from the `STATE_MACHINE_ARN`
  environment variable (wired by CloudFormation to the state machine in this stack).

### Step Functions State Machine

- **Name:** `mission-deh-hof-lyft-pipeline` (STANDARD)
- **Flow:** `RunIngestion` → `RunRawToCurated` → `RunCuratedToAggregated` → `NotifySuccess` → `Succeed`
- **Error handling:** every task has a `Catch` on `States.ALL` that routes to
  `NotifyFailure` (publishes an SNS message including the error cause) and then a
  `Fail` state.
- Uses `glue:startJobRun.sync` so each Glue job runs to completion before the next
  starts.

### SNS Alerts

- **Topic:** `mission-deh-hof-pipeline-alerts`
- **Subscription:** email → `connect@dataengineeringhub.in` (must be confirmed via
  the link SNS sends before alerts are delivered).
- Publishes on both success and failure of the pipeline.

---

## S3 Data Layout

All under the data bucket `mission-deh-hof-nyctlc-live-<ACCOUNT_ID>`:

```
scripts/                                     # Glue job scripts
raw/nyctlc/fhvhv_tripdata/                   # ingested HVFHV parquet (6 months)
raw/nyctlc/taxi_zone_lookup/                 # zone lookup CSV
raw/nyctlc/ingestion.done                    # marker written after ingestion
rejected/nyctlc/fhvhv_trips_rejected/        # DQ-rejected rows (with reason)
curated/nyctlc/fhvhv_trips_curated/          # cleaned Lyft trips (part. by pickup_date)
aggregated/nyctlc/daily_borough_summary/     # aggregate 1 (part. by pickup_date)
aggregated/nyctlc/hourly_pattern_summary/    # aggregate 2
aggregated/nyctlc/route_summary/             # aggregate 3
```

---

## Audit Logging

Each Glue job writes an audit record to the DynamoDB table
**`mission-deh-hof-job-audit-live`** on completion (success or failure), including:
`job_name`, `run_timestamp`, `layer`, `records_read`, `records_written`,
`records_rejected`, `duration_seconds`, `status`, and `error_message`.

---

## Infrastructure (CloudFormation)

Template: [`cloudformation/lyft-pipeline.yaml`](cloudformation/lyft-pipeline.yaml)

### Resources created by the stack
This stack is **self-contained** — it provisions everything it needs. Every named
resource carries a `-live` suffix so it won't collide with other pipeline stacks
in the same account.
- `AWS::S3::Bucket` — the data + scripts bucket (`mission-deh-hof-nyctlc-live-<ACCOUNT_ID>`)
- `AWS::DynamoDB::Table` — the job audit table (`mission-deh-hof-job-audit-live`)
- `AWS::IAM::Role` × 3 — the Glue, Lambda, and Step Functions roles (`...-role-live`)
- `AWS::SNS::Topic` — alerts topic + email subscription
- `AWS::Glue::Job` × 3 — the ingestion and two ETL jobs
- `AWS::StepFunctions::StateMachine` — the orchestrator
- `AWS::Lambda::Function` — the S3 trigger (inline code via `ZipFile`)
- `AWS::Lambda::Permission` — allows S3 to invoke the Lambda

### Parameters (configuration only — no pre-existing infrastructure needed)
| Parameter | Default | Purpose |
|-----------|---------|---------|
| `AlertEmailAddress` | `connect@dataengineeringhub.in` | SNS email subscriber |
| `IngestionScriptKey` | `scripts/backup-nyctlc-ingestion.py` | S3 key of stage-1 script |
| `RawToCuratedScriptKey` | `scripts/backup-raw-to-curated.py` | S3 key of stage-2 script |
| `CuratedToAggregatedScriptKey` | `scripts/backup-curated-to-aggregated.py` | S3 key of stage-3 script |

### Outputs
`DataBucketName`, `JobAuditTableName`, `StateMachineArn`, `AlertsTopicArn`,
`TriggerFunctionArn`, and `S3NotificationNote` (a reminder about the manual
S3-notification step, below).

---

## Deployment

> In this course the deployment is driven automatically by the GitHub Actions
> workflow in `.github/workflows/deploy.yml` (see the hands-on labs). The steps
> below are the equivalent manual commands if you want to deploy from your own
> machine.

### Prerequisites
- AWS CLI configured for the target account/region (`us-east-1`).
- `CAPABILITY_NAMED_IAM` — the stack creates named IAM roles, so this capability
  is required.
- The Glue scripts must be uploaded to the stack's bucket under `scripts/`
  (the CI/CD workflow does this with `aws s3 sync` after the first deploy).

### 1. Validate
```bash
aws cloudformation validate-template \
  --template-body file://cloudformation/lyft-pipeline.yaml
```

### 2. Deploy
```bash
aws cloudformation deploy \
  --template-file cloudformation/lyft-pipeline.yaml \
  --stack-name mission-deh-hof-lyft-pipeline-live \
  --capabilities CAPABILITY_NAMED_IAM
```
(Override defaults with `--parameter-overrides Key=Value ...` as needed.)

### 3. Confirm the SNS email subscription
Click the confirmation link sent to `AlertEmailAddress`, otherwise alerts won't
be delivered.

### 4. Wire the S3 event notification (manual)
CloudFormation can't set a bucket's own notification config in the same stack that
creates the bucket (it would be circular), so add it afterwards. On the stack's
data bucket, add an S3 event notification targeting the
`mission-deh-hof-pipeline-trigger-live` Lambda. **Scope it to a dedicated landing
prefix, not the `raw/` prefix**, to avoid the ingestion job's own writes
re-triggering the pipeline (see below). The Lambda invoke permission is already
granted by the stack.

---

## Branching & Contribution Workflow

This repository uses:
- **master** — stable / production-ready code
- **develop** — ongoing development branch

Intended flow (reviewed at each gate):
1. Branch off `develop` for a feature (e.g. `lyft-pipeline-ingestion`).
2. Open a pull request into **`develop`** → code review → merge.
3. Open a pull request from **`develop`** into **`master`** → review → merge (release).

> Note: `master` is currently the GitHub default branch, so when opening a PR make
> sure the base is set explicitly to `develop` for feature merges.

---

## Design Notes & Known Considerations

- **Avoid a trigger loop.** The ingestion job writes into `raw/`, and the Lambda
  fires on S3 object creation. If the S3 notification is scoped to `raw/`, the
  ingestion output would re-trigger the pipeline indefinitely. Trigger on a separate
  landing prefix or specifically on the `ingestion.done` marker instead.
- **State machine ↔ Lambda wiring.** The template points the Lambda's
  `STATE_MACHINE_ARN` at the `mission-deh-hof-lyft-pipeline-live` state machine defined
  in the same stack, keeping the trigger and the pipeline consistent.
- **Sync Glue integration.** Because tasks use `startJobRun.sync`, the Step Functions
  role (created by the stack) includes the EventBridge-managed rule permissions AWS
  uses to poll Glue job status, in addition to `glue:StartJobRun` / `glue:GetJobRun`.
- **Retries.** The reference ASL in `step-functions/` and the deployed definition
  focus on catch-and-notify. Add per-task `Retry` blocks for transient Glue errors
  if desired.
```
