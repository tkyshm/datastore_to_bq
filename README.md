# Cloud Function to export datastore backup and load into bigquery.

## deploy

```
$ gcloud functions deploy datastore_to_bq \
    --entry-point DatastoreToBQ \
    --runtime go111 \
    --set-env-vars 'PROJECT_ID=...,BUCKET=...' \
    --trigger-http \
    --project ... \
    --region ...
```

## Environment variables

name | desc 
--- | --- 
PROJECT_ID | gcp project
BUCKET | GCS Bucket
DATASET | Bigquery dataset
KIND | Datastore kind
NAMESPACE | Datastore namespace


