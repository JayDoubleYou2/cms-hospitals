# CMS Hospitals Data Downloader

## Data Source

This project uses publicly available datasets from the CMS Provider Data
Catalog. The project is not affiliated with or endorsed by the Centers for
Medicare & Medicaid Services (CMS).

## What this code does

The code connects to the CMS Provider Data Catalog Metastore and selects each
dataset with a `theme` containing `Hospitals`. CMS documents that the metastore
contains dataset metadata and that each dataset's `distribution` array
contains downloadable formats, including CSV and a direct `downloadURL`.

For each selected dataset, it does the following:

1. Reads the CMS `modified` timestamp and CSV `downloadURL`
2. Compares that timestamp with metadata saved from the previous run for that dataset ID
3. Downloads only new or changed datasets
4. Processes changed or new datasets concurrently using
   `ThreadPoolExecutor` - up to 8 at a time
5. Streams each CSV row-by-row instead of loading the entire file into memory
6. Converts headers to lowercase in the snake_case format
7. If column names from the same dataset convert to the same snake_case name, appends an underscore and index to the result
8. Replaces the output only after a download completes successfully.
9. Maintains metadata in `metadata.json`
10. Returns a non-zero exit code if one or more datasets fail while allowing the remaining datasets to continue processing.

The state is updated only after successful processing. Therefore, a failed
dataset remains eligible for retry on the next daily run.

## Example header transformations

Input:

`Patients’ rating of the facility linear mean score`

Output:

`patients_rating_of_the_facility_linear_mean_score`

## Running

No third-party packages are required.

```text
python cms_hospitals.py
```

Optional:

```text
python cms_hospitals.py --output-dir output --metadata-file metadata.json --workers 8
```

Schedule that command daily with Windows Task Scheduler, Linux cron, or MacOS launchd.

## Concurrent threads

This job is not resource-intensive from a CPU computation perspective. Instead most of the time is spent waiting for CMS. A `ThreadPoolExecutor` therefore provides straightforward parallel downloads while remaining cross-platform and requiring no cloud-specific infrastructure.

## Incremental-load design

`metadata.json` is keyed by CMS dataset identifier. Each successful entry stores
the CMS `modified` value, source URL, output path, row count, and processing
timestamp. A dataset is downloaded when:

- it has never been processed;
- its CMS `modified` value changed; or
- its expected output file is missing.

This makes the job safe to run repeatedly and avoids unnecessary downloads.

## CMS API reference

The official CMS API documentation describes the metastore endpoints and the
dataset metadata structure. CMS's current API FAQ also documents that
`distribution` contains available formats and `downloadURL` provides the
direct download link.

The implementation intentionally uses the API URL supplied in the interview
prompt rather than hard-coding a list of Hospital dataset IDs.
