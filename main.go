package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"

	"github.com/slingdata-io/sling-cli/core/sling"

	"gopkg.in/yaml.v3"
)

type GCSConfig struct {
    ProjectID string   `yaml:"projectID"`
    Schema    string   `yaml:"schema"`
    Timezone  string   `yaml:"timezone"`
    Sources   map[string]Source `yaml:"sources"`
}

type Source struct {
    TablePrefix       string `yaml:"tablePrefix"`
    Bucket            string `yaml:"bucket"`
    BucketSuffix      string `yaml:"bucketSuffix"`
    FileFormat        string `yaml:"fileFormat"`
    ReplicationScheme string `yaml:"replicationScheme"`
    DateRangeStart    string `yaml:"dateRangeStart"`
    DateRangeEnd      string `yaml:"dateRangeEnd"`
    SlingCfgPath      string `yaml:"slingCfgPath"`
}

// exportTableAsCompressedCSV demonstrates using an export job to
// write the contents of a table into Cloud Storage as CSV.
func exportTableAsShardedJSON(srcProjectID string, srcDataset string, srcTable string, gcsPath string) error {
    // projectID := "my-project-id"
    // gcsPath := "gs://mybucket"
    ctx := context.Background()
    client, err := bigquery.NewClient(ctx, srcProjectID)
    if err != nil {
        return fmt.Errorf("bigquery.NewClient: %v", err)
    }
    defer client.Close()

    gcsURI := fmt.Sprintf("%s/%s_*.json", gcsPath, srcTable)

    gcsRef := bigquery.NewGCSReference(gcsURI)
    gcsRef.DestinationFormat = bigquery.JSON

    extractor := client.DatasetInProject(srcProjectID, srcDataset).Table(srcTable).ExtractorTo(gcsRef)
    extractor.Location = "US"

    job, err := extractor.Run(ctx)
    if err != nil {
        return err
    }
    status, err := job.Wait(ctx)
    if err != nil {
        return err
    }
    if err := status.Err(); err != nil {
        return err
    }

    log.Printf("%s created. \n", gcsURI)
    return nil
}

// listFilesWithPrefix lists objects using prefix and delimeter.
func listFilesWithPrefix(bucket, prefix, delim string) ([]string, error) {
    // bucket := "bucket-name"
    // prefix := "/foo"
    // delim := "_"
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        return nil, fmt.Errorf("storage.NewClient: %w", err)
    }
    defer client.Close()

    // Prefixes and delimiters can be used to emulate directory listings.
    // Prefixes can be used to filter objects starting with prefix.
    // The delimiter argument can be used to restrict the results to only the
    // objects in the given "directory". Without the delimiter, the entire tree
    // under the prefix is returned.
    //
    // For example, given these blobs:
    //   /a/1.txt
    //   /a/b/2.txt
    //
    // If you just specify prefix="a/", you'll get back:
    //   /a/1.txt
    //   /a/b/2.txt
    //
    // However, if you specify prefix="a/" and delim="/", you'll get back:
    //   /a/1.txt
    ctx, cancel := context.WithTimeout(ctx, time.Second*10)
    defer cancel()

    it := client.Bucket(bucket).Objects(ctx, &storage.Query{
        Prefix:    prefix,
        Delimiter: delim,
    })

    var bucketList []string
    for {
        attrs, err := it.Next()
        if err == iterator.Done {
            break
        }
        if err != nil {
            return nil, fmt.Errorf("Bucket(%q).Objects(): %w", bucket, err)
        }
        bucketList = append(bucketList, attrs.Name)
    }
    return bucketList, nil
}

// deleteFile removes specified object.
func deleteFile(bucket, object string) error {
    // bucket := "bucket-name"
    // object := "object-name"
    ctx := context.Background()
    client, err := storage.NewClient(ctx)
    if err != nil {
        return fmt.Errorf("storage.NewClient: %w", err)
    }
    defer client.Close()

    ctx, cancel := context.WithTimeout(ctx, time.Second*10)
    defer cancel()

    o := client.Bucket(bucket).Object(object)

    // Optional: set a generation-match precondition to avoid potential race
    // conditions and data corruptions. The request to delete the file is aborted
    // if the object's generation number does not match your precondition.
    attrs, err := o.Attrs(ctx)
    if err != nil {
        return fmt.Errorf("object.Attrs: %w", err)
    }
    o = o.If(storage.Conditions{GenerationMatch: attrs.Generation})

    if err := o.Delete(ctx); err != nil {
        return fmt.Errorf("Object(%q).Delete: %w", object, err)
    }
    log.Printf("Blob %v deleted.\n", object)
    return nil
}

func parseGCSBucketContents(contents []string, fileFormatSuffix string) ([]string, error) {
    var matchingFiles []string
    for i := range contents {
        gcsObject := contents[i]
        match, _ := regexp.MatchString(
            fmt.Sprintf(`.*\.%s$`, fileFormatSuffix),
            gcsObject,
        )
        if match {
            matchingFiles = append(matchingFiles, gcsObject)
        }
    }

    if len(matchingFiles) == 0 {
        return matchingFiles, fmt.Errorf("Input slice has no files of %s format", fileFormatSuffix)
    }
    return matchingFiles, nil
}

func emptyBucketDirectory(bucketName string, filePrefix string, fileFormatSuffix string) error {
    bucketContents, err := listFilesWithPrefix(
        bucketName,
        filePrefix,
        "/",
    )
    if err != nil {
        return err
    }

    match, err := parseGCSBucketContents(bucketContents, fileFormatSuffix)
    if err != nil {
        return err
    }

    for i := range match {
        deleteFile(bucketName, match[i])
    }
    return nil
}

func invokeSling(cfgStr string) {
	cfg, err := sling.NewConfig(cfgStr)
	if err != nil {
		log.Fatal(err)
	}

	err = sling.Sling(cfg)
	if err != nil {
		log.Fatal(err)
	}
}

func makeGCSPath(bucketName string, bucketSuffix string) string {
    return fmt.Sprintf("gs://%s/%s", bucketName, bucketSuffix)
}

func createDate(timezone string, replicationTarget string) string {
    now := time.Now().UTC()

    if replicationTarget == "daily" {
        now = now.Add(-24 * time.Hour)
    }

    tz, err := time.LoadLocation(timezone)
    if err != nil {
        log.Println("Error parsing timezone string. Defaulting to UTC")
        return now.Format("20060102")
    }

    return now.In(tz).Format("20060102")
}

func generateDateRange(dateStartStr string, dateEndStr string) []string {
    dateStart, err := time.Parse("20060102", dateStartStr)
    if err != nil {
        log.Fatalf("dataRangeStart not in YYYYMMDD format")
    }

    dateEnd, err := time.Parse("20060102", dateEndStr)
    if err != nil {
        log.Fatalf("dataRangeEnd not in YYYYMMDD format")
    }

    dateSlice := []string{dateStartStr, dateEndStr}

    diff := dateEnd.Sub(dateStart)

    daysDiff := int(diff.Hours() / 24)

    for i := 1; i < daysDiff; i++ {
        dateSlice = append(
            dateSlice,
            dateStart.AddDate(0, 0, i).Format("20060102"),
        )
    }

    return dateSlice
}

func (gcs *GCSConfig) dateRange(key string) ([]string, error) {
    var dateRangeSlice []string
    if gcs.Sources[key].ReplicationScheme == "today" {
        dateRangeSlice = append(dateRangeSlice, createDate(gcs.Timezone, key))
    } else if gcs.Sources[key].ReplicationScheme == "range" {
        dateRangeSlice = append(
            dateRangeSlice,
            generateDateRange(
                gcs.Sources[key].DateRangeStart,
                gcs.Sources[key].DateRangeEnd,
            )...,
        )
    } else {
        return nil, fmt.Errorf("Replication scheme must be one of these options [today, range]")
    }

    return dateRangeSlice, nil
}

func main() {

    var gcs GCSConfig

    gcsCfgYaml, err := os.ReadFile("./gcs.yaml")
    if err != nil {
        log.Fatal(err)
    }

    err = yaml.Unmarshal(gcsCfgYaml, &gcs)
    if err != nil {
        log.Fatal(err)
    }

    repl := []string{"daily", "intraday"}

    for i := range repl {
        key := repl[i]

        slingCfg, err := os.ReadFile(gcs.Sources[key].SlingCfgPath)
        if err != nil {
            log.Fatal(err)
        }

        dateRange, err := gcs.dateRange(key)
        if err != nil {
            log.Fatal(err)
        }

        for d := range dateRange {
            srcTable := fmt.Sprintf(
                "%s%s",
                gcs.Sources[key].TablePrefix,
                dateRange[d],
            )

            err = exportTableAsShardedJSON(
                gcs.ProjectID,
                gcs.Schema,
                srcTable,
                makeGCSPath(gcs.Sources[key].Bucket, gcs.Sources[key].BucketSuffix),
            )
            if err != nil {
                log.Fatalf("Error exporting table: %s", err)
            }
        }

        invokeSling(string(slingCfg))

        err = emptyBucketDirectory(
            gcs.Sources[key].Bucket,
            gcs.Sources[key].BucketSuffix,
            gcs.Sources[key].FileFormat,
        )
        if err != nil {
            log.Fatalf("Error deleting files: %s", err)
        }
    }
}
