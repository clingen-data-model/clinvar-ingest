"""
Refresh the gene reference tables that clinvar_ingest.single_gene_variation uses.

Downloads two NCBI Gene files, keeps the human rows, loads them into the staging tables
<dataset>.entrez_gene_stage and <dataset>.ncbi_gene_history_stage, then calls the stored
procedure <dataset>.gene_reference_update from clinvar-ingest-bq-tools
(scripts/general/gene-reference-update-proc.sql). That procedure applies add-only changes to
entrez_gene and ncbi_gene_history and logs the run to gene_reference_refresh_log.

clinvar_ingest.validate_dataset fails a release when no refresh is logged on or after its
release date, so the stored-procedures workflow runs this before claiming any release.
See docs/methods/single-gene-variation.md in clinvar-ingest-bq-tools.
"""

import datetime
import gzip
import io
import logging
import tempfile
import time
from collections.abc import Iterable, Iterator
from pathlib import Path

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

_logger = logging.getLogger("clinvar_ingest")

GENE_INFO_URL = "https://ftp.ncbi.nih.gov/gene/DATA/GENE_INFO/Mammalia/Homo_sapiens.gene_info.gz"
GENE_HISTORY_URL = "https://ftp.ncbi.nih.gov/gene/DATA/gene_history.gz"
HUMAN_TAX_ID = "9606"

ENTREZ_GENE_STAGE_SCHEMA = [
    bigquery.SchemaField("gene_id", "STRING"),
    bigquery.SchemaField("symbol_from_authority", "STRING"),
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("hgnc_id", "STRING"),
    bigquery.SchemaField("type_of_gene", "STRING"),
    bigquery.SchemaField("description", "STRING"),
    bigquery.SchemaField("has_hgnc", "BOOL"),
]

GENE_HISTORY_STAGE_SCHEMA = [
    bigquery.SchemaField("discontinued_gene_id", "STRING"),
    bigquery.SchemaField("replaced_by_gene_id", "STRING"),
    bigquery.SchemaField("discontinued_symbol", "STRING"),
    bigquery.SchemaField("discontinue_date", "DATE"),
]


def parse_gene_info(lines: Iterable[str]) -> Iterator[tuple[str, ...]]:
    """
    Yield the human rows of an NCBI gene_info file as entrez_gene_stage rows:
    (gene_id, symbol_from_authority, symbol, hgnc_id, type_of_gene, description, has_hgnc).

    has_hgnc is "true" when the gene has an HGNC symbol and an HGNC id; only those
    genes are added to entrez_gene.
    """
    for line in lines:
        fields = line.rstrip("\n").split("\t")
        # also skips the "#tax_id ..." header line
        if fields[0] != HUMAN_TAX_ID:
            continue
        hgnc_id = ""
        for ref in fields[5].split("|"):
            if ref.startswith("HGNC:HGNC:"):
                hgnc_id = ref.removeprefix("HGNC:")
        symbol_from_authority = "" if fields[10] == "-" else fields[10]
        has_hgnc = "true" if symbol_from_authority and hgnc_id else "false"
        yield (fields[1], symbol_from_authority, fields[2], hgnc_id, fields[9], fields[8], has_hgnc)


def parse_gene_history(lines: Iterable[str]) -> Iterator[tuple[str, ...]]:
    """
    Yield the human rows of an NCBI gene_history file as ncbi_gene_history_stage rows:
    (discontinued_gene_id, replaced_by_gene_id, discontinued_symbol, discontinue_date).
    replaced_by_gene_id is "" when NCBI gives no replacement ("-").
    """
    for line in lines:
        fields = line.rstrip("\n").split("\t")
        if fields[0] != HUMAN_TAX_ID:
            continue
        replaced_by = "" if fields[1] == "-" else fields[1]
        yyyymmdd = fields[4]
        yield (fields[2], replaced_by, fields[3], f"{yyyymmdd[0:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}")


def download(url: str, dest: Path, attempts: int = 3, timeout: tuple[int, int] = (10, 300)) -> None:
    """Stream url to dest, retrying failed requests with a growing pause."""
    for attempt in range(1, attempts + 1):
        try:
            with requests.get(url, stream=True, timeout=timeout) as response:
                response.raise_for_status()
                with dest.open("wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        f.write(chunk)
        except requests.RequestException as e:
            if attempt == attempts:
                raise
            _logger.warning(f"Download of {url} failed (attempt {attempt} of {attempts}): {e}")
            time.sleep(10 * attempt)
        else:
            return


def load_stage(
    client: bigquery.Client,
    table_id: str,
    rows: Iterable[tuple[str, ...]],
    schema: list[bigquery.SchemaField],
) -> int:
    """Replace table_id with rows, loaded as tab-separated values. Returns the rows loaded."""
    data = "".join("\t".join(row) + "\n" for row in rows).encode("utf-8")
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        field_delimiter="\t",
        quote_character="",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    job = client.load_table_from_file(io.BytesIO(data), table_id, job_config=job_config)
    job.result()
    return job.output_rows


def refresh_gene_reference(client: bigquery.Client, project: str, dataset: str, triggered_by: str) -> dict:
    """
    Download the NCBI gene files, stage them, and apply the add-only update.
    Returns the counts reported by gene_reference_update.
    """
    with tempfile.TemporaryDirectory() as tmp:
        gene_info_path = Path(tmp) / "gene_info.gz"
        gene_history_path = Path(tmp) / "gene_history.gz"
        download(GENE_INFO_URL, gene_info_path)
        download(GENE_HISTORY_URL, gene_history_path)
        with gzip.open(gene_info_path, "rt", encoding="utf-8") as f:
            gene_rows = list(parse_gene_info(f))
        with gzip.open(gene_history_path, "rt", encoding="utf-8") as f:
            history_rows = list(parse_gene_history(f))

    _logger.info(f"Parsed {len(gene_rows)} human genes and {len(history_rows)} discontinued human gene ids")
    load_stage(client, f"{project}.{dataset}.entrez_gene_stage", gene_rows, ENTREZ_GENE_STAGE_SCHEMA)
    load_stage(client, f"{project}.{dataset}.ncbi_gene_history_stage", history_rows, GENE_HISTORY_STAGE_SCHEMA)

    job = client.query(
        f"CALL `{project}.{dataset}.gene_reference_update`(TRUE, @triggered_by);",
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("triggered_by", "STRING", triggered_by)]
        ),
        project=project,
    )
    rows = list(job.result())
    return dict(rows[0].items()) if rows else {}


def last_refresh(client: bigquery.Client, project: str, dataset: str) -> datetime.datetime | None:
    """When the gene reference tables were last refreshed, or None if never."""
    query = f"SELECT MAX(refreshed_at) AS last_refresh FROM `{project}.{dataset}.gene_reference_refresh_log`"  # noqa: S608
    try:
        rows = list(client.query(query, project=project).result())
    except NotFound:
        return None
    return rows[0]["last_refresh"] if rows else None


def covers_release(refreshed_at: datetime.datetime | None, release_date: datetime.date) -> bool:
    """True when a refresh at refreshed_at satisfies validate_dataset for release_date."""
    return refreshed_at is not None and refreshed_at.date() >= release_date
