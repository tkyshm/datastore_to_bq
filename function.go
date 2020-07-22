package function

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/datastore/v1beta1"
)

func DatastoreToBQ(w http.ResponseWriter, _ *http.Request) {
	ctx := context.Background()

	// Set your Google Cloud Platform project ID.
	projectID := os.Getenv("PROJECT_ID")

	kind := getKind()
	name := getNS()

	// export datastore
	svc, err := datastore.NewService(ctx)
	if err != nil {
		log.Printf("Failed to NewService: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	prefix, err := getOutputGS()
	if err != nil {
		log.Printf("Failed to getOutputGS: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	req := datastore.GoogleDatastoreAdminV1beta1ExportEntitiesRequest{
		EntityFilter: &datastore.GoogleDatastoreAdminV1beta1EntityFilter{
			Kinds:        []string{kind},
			NamespaceIds: []string{name},
		},
		OutputUrlPrefix: prefix,
	}
	call := svc.Projects.Export(projectID, &req)
	if _, err := call.Do(); err != nil {
		log.Printf("Failed to export datastore: %v, path=%s", err, prefix)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// load bigquery
	metadataPath := getMetadataPath(name, kind)
	exportMetaFile := fmt.Sprintf("%s%s", prefix, metadataPath)

	gcsRef := bigquery.NewGCSReference(exportMetaFile)
	gcsRef.AllowJaggedRows = true
	gcsRef.SourceFormat = bigquery.DatastoreBackup

	client, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		log.Printf("failed to create new bigquery client: %v, exportMetaFile=%s", err, exportMetaFile)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	myDataset := client.Dataset(getDataset())
	loader := myDataset.Table(kind).LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateNever
	loader.WriteDisposition = bigquery.WriteTruncate
	job, err := loader.Run(ctx)
	if err != nil {
		log.Printf("failed to run loader: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	status, err := job.Wait(ctx)
	if err != nil {
		log.Printf("failed to run loader: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	log.Printf("done %#v", status.State)
	w.WriteHeader(http.StatusOK)
}

func getOutputGS() (string, error) {
	bucket := os.Getenv("BUCKET")
	now := time.Now()
	date := fmt.Sprintf("%d-%d-%d_%d-%d/", now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Printf("failed to create folder: %v", err)
		return "", err
	}

	dest := fmt.Sprintf("gs://%s/datastore_backups/%s", bucket, date)
	w := client.Bucket(bucket).Object(dest).NewWriter(ctx)
	if _, err := w.Write([]byte{}); err != nil {
		log.Printf("failed to create folder: %v", err)
		return "", err
	}
	return dest, nil
}

func getDataset() string {
	return os.Getenv("DATASET")
}

func getKind() string {
	return os.Getenv("KIND")
}

func getNS() string {
	return os.Getenv("NAMESPACE")
}

func getMetadataPath(ns, kind string) string {
	return fmt.Sprintf("namespace_%s/kind_%s/namespace_%s_kind_%s.export_metadata", ns, kind, ns, kind)
}
