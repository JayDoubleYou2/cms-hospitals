#!/usr/bin/env python3
"""
CMS Hospitals dataset downloader.

This script connects to the CMS Provider Data Catalog Metastore and selects datasets with
themes containing the string "Hospitals" (disregarding case sensitivity).

It downloads only new datasets or datasets changed since the
previous successful run and normalizes CSV headers in the form of snake_case.

This code is Standard-library only - it runs on Windows, Linux, and MacOS. It was tested on MacOS.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import tempfile
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import Request, urlopen

METASTORE_URL = (
    "https://data.cms.gov/provider-data/api/1/"
    "metastore/schemas/dataset/items"
)
THEME = "Hospitals"
DEFAULT_WORKERS = 8


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

# Read metadata from the previous run. If this is the first run 
# and the file doesn't exist, start with an empty metadata structure.
def load_metadata(path: Path) -> dict:
    if not path.exists():
        return {"datasets": {}}
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# Save updated dataset metadata to a JSON file so it can be used
# to determine which datasets need to be downloaded on the next run.
def save_metadata(path: Path, metadata: dict) -> None:
    """Save the run metadata in a way that prevents the file from being left partially written if something goes wrong."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        "w", encoding="utf-8", dir=path.parent, delete=False
    ) as tmp:
        json.dump(metadata, tmp, indent=2, sort_keys=True)
        tmp.write("\n")
        tmp_path = Path(tmp.name)
    tmp_path.replace(path)

# Make request to URL and return JSON data as Python object
def fetch_json(url: str) -> object:
    request = Request(
        url,
        headers={"User-Agent": "cms-hospitals-downloader/1.0"},
    )
    with urlopen(request, timeout=60) as response:
        return json.load(response)

# find all the CMS Hospital datasets we need to process.
# A dataset can have multiple distributions. Find the CSV specifically
# instead of assuming the first available option is the CSV.
# Skip datasets that do not provide a downloadable CSV.
def get_hospital_datasets() -> list[dict]:
    catalog = fetch_json(METASTORE_URL)

    if not isinstance(catalog, list):
        raise RuntimeError("Unexpected CMS metastore response.")

    datasets = []

    for item in catalog:
        themes = item.get("theme") or []

        # Match any theme containing "hospitals", case-insensitively.
        # Examples: "Hospitals", "hospitals", "Hospitals and Clinics".
        normalized_themes = [str(theme).strip().casefold() for theme in themes]
        if not any(THEME.casefold() in theme for theme in normalized_themes):
            continue

        # A dataset can have multiple distributions. find the CSV specifically
        # instead of assuming the first available option is the CSV.
        distributions = item.get("distribution") or []
        csv_distribution = next(
            (
                distribution
                for distribution in distributions
                if str(distribution.get("mediaType", "")).lower()
                == "text/csv"
                and distribution.get("downloadURL")
            ),
            None,
        )

        if not csv_distribution:
            continue

        datasets.append(
            {
                "identifier": item.get("identifier"),
                "title": item.get("title", "untitled"),
                "modified": item.get("modified"),
                "download_url": csv_distribution["downloadURL"],
            }
        )

    return datasets

# Convert arbitrary CSV header text to lower snake_case
def snake_case(value: str) -> str:
    # handle Unicode characters and accents consistently
    value = unicodedata.normalize("NFKD", value)
    # normalize curly apostrophes
    value = value.replace("’", "'").replace("‘", "'")
    # convert & into and
    value = value.replace("&", " and ")
    # remove apostrophes
    value = re.sub(r"['’]", "", value)
    # remove non-ASCII characters
    value = value.encode("ascii", "ignore").decode("ascii")
    # use regex to replace anything that's not a letter or number with _
    value = re.sub(r"[^A-Za-z0-9]+", "_", value)
    # collapse repeated underscores
    value = re.sub(r"_+", "_", value).strip("_")
    # convert everything to lowercase
    return value.lower() or "column"


# Take the original CSV headers, convert each one to snake_case,
# and make sure you don't end up with duplicate column names.
def unique_headers(headers: list[str]) -> list[str]:
    counts: dict[str, int] = {}
    result = []

    for header in headers:
        base = snake_case(header)
        counts[base] = counts.get(base, 0) + 1
        result.append(
            base if counts[base] == 1 else f"{base}_{counts[base]}"
        )

    return result

# Download a CSV file, convert its headers to snake_case, and write the
# processed data to the output path while streaming rows to avoid loading
# the entire file into memory. The output is written to a temporary file
# and only replaces the existing output after processing completes successfully.
def process_csv(source_url: str, output_path: Path) -> int:
    """Download one CSV and stream it to an atomically-created output."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    request = Request(
        source_url,
        headers={"User-Agent": "cms-hospitals-downloader/1.0"},
    )

    with urlopen(request, timeout=300) as response, \
         tempfile.NamedTemporaryFile(
             "w",
             encoding="utf-8",
             newline="",
             dir=output_path.parent,
             delete=False,
         ) as tmp:

        tmp_path = Path(tmp.name)
        reader = csv.reader(
            (
                line.decode("utf-8-sig", errors="replace")
                for line in response
            )
        )
        writer = csv.writer(tmp)

        try:
            headers = next(reader)
        except StopIteration as exc:
            raise RuntimeError("Downloaded CSV is empty.") from exc

        writer.writerow(unique_headers(headers))

        row_count = 0
        for row in reader:
            writer.writerow(row)
            row_count += 1

    # Replace the prior output only after the complete file is processed.
    tmp_path.replace(output_path)
    return row_count

# decide what filename to use for each downloaded dataset.
def output_filename(dataset: dict) -> str:
    return f"{dataset['identifier'] or 'unknown'}.csv"

# determine where csv will go, download and process, return metadata about successful processing
def process_dataset(dataset: dict, output_dir: Path) -> dict:
    output_path = output_dir / output_filename(dataset)

    rows = process_csv(dataset["download_url"], output_path)

    return {
        "identifier": dataset["identifier"],
        "title": dataset["title"],
        "modified": dataset["modified"],
        "download_url": dataset["download_url"],
        "output_file": str(output_path),
        "rows": rows,
        "processed_at": utc_now(),
    }

# retrieves the Hospital datasets from the CMS metastore, compares their current modification timestamps against metadata from the previous run, 
# and identify only the datasets that are new, modified, or missing locally. Processes those datasets concurrently using a thread pool, record
# successful and failed datasets, update the run metadata, and return a non-zero exit code if any datasets failed.
def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="output")
    parser.add_argument("--metadata-file", default="metadata.json")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS)
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    metadata_path = Path(args.metadata_file)

    datasets = get_hospital_datasets()
    metadata = load_metadata(metadata_path)
    prior = metadata.setdefault("datasets", {})
    
    changed = [
        dataset
        for dataset in datasets
        if (
            dataset["identifier"] not in prior
            or dataset.get("modified")
            != prior[dataset["identifier"]].get("modified")
            or not (
                output_dir / output_filename(dataset)
            ).exists()
        )
    ]

    print(f"Hospital datasets found: {len(datasets)}")
    print(f"Datasets requiring download: {len(changed)}")

    failures = []
    successful = 0

    if not changed:
        metadata["last_run_at"] = utc_now()
        metadata["datasets_seen"] = len(datasets)
        metadata["successful_this_run"] = successful
        metadata["failed_this_run"] = failures
        save_metadata(metadata_path, metadata)
        print("No changed datasets. Nothing downloaded.")
        return 0

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {
            executor.submit(
                process_dataset, dataset, output_dir
            ): dataset
            for dataset in changed
        }

        for future in as_completed(futures):
            dataset = futures[future]

            try:
                result = future.result()

                # Only record metadata after the file fully succeeds.
                prior[result["identifier"]] = result
                successful += 1

                print(
                    f"OK  {result['identifier']} | "
                    f"{result['title']} | "
                    f"{result['rows']:,} rows"
                )
            except Exception as exc:
                failures.append(
                    {
                        "identifier": dataset["identifier"],
                        "title": dataset["title"],
                        "error": str(exc),
                    }
                )
                print(
                    f"ERROR {dataset['identifier']} | "
                    f"{dataset['title']} | {exc}"
                )

    metadata["last_run_at"] = utc_now()
    metadata["datasets_seen"] = len(datasets)
    metadata["successful_this_run"] = successful
    metadata["failed_this_run"] = failures
    save_metadata(metadata_path, metadata)

    print(f"Successful: {successful}")
    print(f"Failed: {len(failures)}")

    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
