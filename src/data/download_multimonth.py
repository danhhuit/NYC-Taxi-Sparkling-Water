"""Download and validate the configured NYC TLC Yellow Taxi Parquet files."""

from __future__ import annotations

import argparse
import calendar
import sys
import urllib.request
from datetime import date
from pathlib import Path

from src.common.project import load_config, project_path


def month_range(start: str, end: str) -> list[str]:
    start_year, start_month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))
    cursor = date(start_year, start_month, 1)
    stop = date(end_year, end_month, 1)
    result: list[str] = []
    while cursor <= stop:
        result.append(cursor.strftime("%Y-%m"))
        days = calendar.monthrange(cursor.year, cursor.month)[1]
        cursor = date.fromordinal(cursor.toordinal() + days)
    return result


def valid_parquet(path: Path) -> bool:
    if not path.is_file() or path.stat().st_size < 8:
        return False
    with path.open("rb") as stream:
        header = stream.read(4)
        stream.seek(-4, 2)
        footer = stream.read(4)
    return header == b"PAR1" and footer == b"PAR1"


def download(url: str, destination: Path, force: bool) -> None:
    if valid_parquet(destination) and not force:
        print(f"SKIP  {destination.name} ({destination.stat().st_size / 1_048_576:.2f} MB)")
        return

    temporary = destination.with_suffix(destination.suffix + ".part")
    if temporary.exists():
        temporary.unlink()

    print(f"GET   {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "NYC-Taxi-Final-Project/1.0"})
    with urllib.request.urlopen(request, timeout=120) as response:
        expected = int(response.headers.get("Content-Length", "0"))
        with temporary.open("wb") as output:
            while True:
                chunk = response.read(1024 * 1024)
                if not chunk:
                    break
                output.write(chunk)

    if expected and temporary.stat().st_size != expected:
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Incomplete download: {destination.name}")
    if not valid_parquet(temporary):
        temporary.unlink(missing_ok=True)
        raise RuntimeError(f"Invalid Parquet file: {destination.name}")

    temporary.replace(destination)
    print(f"OK    {destination.name} ({destination.stat().st_size / 1_048_576:.2f} MB)")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default=None)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()

    config = load_config(args.config)
    dataset = config["dataset"]
    output_dir = project_path(config["paths"]["raw_yellow"])
    output_dir.mkdir(parents=True, exist_ok=True)

    months = month_range(dataset["start_month"], dataset["end_month"])
    template = dataset["download_url_template"]
    print(f"Downloading {len(months)} month(s): {months[0]} -> {months[-1]}")

    failures: list[str] = []
    for month in months:
        destination = output_dir / f"yellow_tripdata_{month}.parquet"
        try:
            download(template.format(month=month), destination, args.force)
        except Exception as exc:
            failures.append(f"{month}: {exc}")
            print(f"ERROR {month}: {exc}", file=sys.stderr)

    if failures:
        print("\nDownload failures:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1

    print("\nMULTI-MONTH DOWNLOAD SUCCESS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
