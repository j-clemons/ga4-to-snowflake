package main

import (
	"reflect"
	"testing"
)

func TestGenerateDateRange(t *testing.T) {
    startDate := "20230101"
    endDate := "20230105"
    expected := []string{"20230101", "20230102", "20230103", "20230104", "20230105"}
    result := generateDateRange(startDate, endDate)
    if !reflect.DeepEqual(result, expected) {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

func TestMakeGCSPath(t *testing.T) {
    expected := "gs://your-bucket-name/bucket-suffix"
    result := makeGCSPath("your-bucket-name", "bucket-suffix")
    if result != expected {
        t.Errorf("Expected %s, got %s", expected, result)
    }
}

func testSelectTables(t *testing.T) {
    gcs := GCSConfig{
        ProjectID:      "your-project-id",
        Schema:         "your-schema",
        Timezone:       "UTC",
        ExportStrategy: "daily+streaming",
        Sources:        map[string]Source{
            "daily": {
                TablePrefix:       "events_",
                Bucket:            "suffix1",
                BucketSuffix:      "suffix1",
                FileFormat:        "json",
                ReplicationScheme: "range",
                DateRangeStart:    "20230101",
                DateRangeEnd:      "20230102",
                SlingCfgPath:      "path/to/sling.yaml",
            },
            "intraday": {
                TablePrefix:       "events_intraday_",
                Bucket:            "suffix1",
                BucketSuffix:      "suffix1",
                FileFormat:        "json",
                ReplicationScheme: "today",
                DateRangeStart:    "20230101",
                DateRangeEnd:      "20230105",
                SlingCfgPath:      "path/to/sling.yaml",
            },
        },
    }

    availableTables := []string{"events_20250101", "events_20250102", "events_20250103"}

    expected := []string{"events_20250101", "events_20250102"}

    result, err := gcs.selectTables("daily", availableTables)
    if err != nil {
        t.Errorf("Error selecting tables: %v", err)
    }
    if !reflect.DeepEqual(result, expected) {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

func TestParseGCSBucketContents(t *testing.T) {
    bucketContents := []string{
        "gs://your-bucket-name/file1.json",
        "gs://your-bucket-name/file2.json",
        "gs://your-bucket-name/file3.txt",
    }
    expected := []string{"gs://your-bucket-name/file1.json", "gs://your-bucket-name/file2.json"}
    result, err := parseGCSBucketContents(bucketContents, "json")
    if err != nil {
        t.Errorf("Error parsing GCS bucket contents: %v", err)
    }
    if !reflect.DeepEqual(result, expected) {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}

func TestListFilesWithPrefix(t *testing.T) {
    bucketName := "gcp-public-data-nexrad-l3"
    prefix := "1992/05/07/KLWX"
    delimeter := ""

    expected := []string{"1992/05/07/KLWX/NWS_NEXRAD_NXL3_KLWX_19920507000000_19920507235959.tar.Z"}
    result, err := listFilesWithPrefix(bucketName, prefix, delimeter)
    if err != nil {
        t.Errorf("Error listing files with prefix: %v", err)
    }
    if !reflect.DeepEqual(result, expected) {
        t.Errorf("Expected %v, got %v", expected, result)
    }
}
