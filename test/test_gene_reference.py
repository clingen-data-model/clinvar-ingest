import datetime
import gzip
from pathlib import Path
from unittest.mock import MagicMock, patch

from google.cloud import bigquery

from clinvar_ingest.cloud.bigquery import gene_reference

GENE_INFO_LINES = [
    "#tax_id\tGeneID\tSymbol\tLocusTag\tSynonyms\tdbXrefs\tchromosome\tmap_location\tdescription\ttype_of_gene\tSymbol_from_nomenclature_authority\tFull_name_from_nomenclature_authority\tNomenclature_status\tOther_designations\tModification_date\tFeature_type\n",
    "9606\t3949\tLDLR\t-\tFH|FHC\tMIM:606945|HGNC:HGNC:6547|Ensembl:ENSG00000130164|AllianceGenome:HGNC:6547\t19\t19p13.2\tlow density lipoprotein receptor\tprotein-coding\tLDLR\tlow density lipoprotein receptor\tO\t-\t20261013\t-\n",
    "9606\t100133077\tLOC100133077\t-\t-\t-\t1\t-\tuncharacterized LOC100133077\tncRNA\t-\t-\t-\t-\t20250101\t-\n",
    "10090\t16835\tLdlr\t-\t-\tMGI:MGI:96765\t9\t-\tlow density lipoprotein receptor\tprotein-coding\tLdlr\t-\t-\t-\t20261013\t-\n",
]

GENE_HISTORY_LINES = [
    "#tax_id\tGeneID\tDiscontinued_GeneID\tDiscontinued_Symbol\tDiscontinue_Date\n",
    "9606\t-\t285464\tCRIPAK\t20190702\n",
    "9606\t399687\t9220\tTIAF1\t20220111\n",
    "10090\t-\t12345\tFoo\t20200101\n",
]


def test_parse_gene_info_keeps_human_rows_and_flags_hgnc_genes():
    rows = list(gene_reference.parse_gene_info(GENE_INFO_LINES))
    assert rows == [
        ("3949", "LDLR", "LDLR", "HGNC:6547", "protein-coding", "low density lipoprotein receptor", "true"),
        ("100133077", "", "LOC100133077", "", "ncRNA", "uncharacterized LOC100133077", "false"),
    ]


def test_parse_gene_history_keeps_human_rows_and_formats_dates():
    rows = list(gene_reference.parse_gene_history(GENE_HISTORY_LINES))
    assert rows == [
        ("285464", "", "CRIPAK", "2019-07-02"),
        ("9220", "399687", "TIAF1", "2022-01-11"),
    ]


def test_load_stage_replaces_table_with_tab_separated_rows():
    client = MagicMock()
    client.load_table_from_file.return_value.output_rows = 2

    loaded = gene_reference.load_stage(
        client,
        "proj.ds.entrez_gene_stage",
        [("1", "A"), ("2", "B")],
        [bigquery.SchemaField("gene_id", "STRING"), bigquery.SchemaField("symbol", "STRING")],
    )

    assert loaded == 2
    data, table_id = client.load_table_from_file.call_args.args
    job_config = client.load_table_from_file.call_args.kwargs["job_config"]
    assert table_id == "proj.ds.entrez_gene_stage"
    assert data.read() == b"1\tA\n2\tB\n"
    assert job_config.field_delimiter == "\t"
    assert job_config.quote_character == ""
    assert job_config.write_disposition == bigquery.WriteDisposition.WRITE_TRUNCATE


def _write_gz(path: Path, lines: list[str]) -> None:
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.writelines(lines)


def test_refresh_gene_reference_stages_both_files_and_calls_the_procedure():
    def fake_download(url, dest, **_kwargs):
        _write_gz(dest, GENE_INFO_LINES if url == gene_reference.GENE_INFO_URL else GENE_HISTORY_LINES)

    client = MagicMock()
    client.query.return_value.result.return_value = [{"applied": True, "entrez_genes_to_insert": 0}]

    with patch.object(gene_reference, "download", side_effect=fake_download):
        result = gene_reference.refresh_gene_reference(client, "proj", "ds", "test run")

    staged_tables = [call.args[1] for call in client.load_table_from_file.call_args_list]
    assert staged_tables == ["proj.ds.entrez_gene_stage", "proj.ds.ncbi_gene_history_stage"]

    query = client.query.call_args.args[0]
    params = client.query.call_args.kwargs["job_config"].query_parameters
    assert query == "CALL `proj.ds.gene_reference_update`(TRUE, @triggered_by);"
    assert params[0].value == "test run"
    assert result == {"applied": True, "entrez_genes_to_insert": 0}


def test_covers_release_requires_a_refresh_on_or_after_the_release_date():
    release = datetime.date(2026, 10, 11)
    utc = datetime.UTC
    assert gene_reference.covers_release(datetime.datetime(2026, 10, 11, 0, 5, tzinfo=utc), release)
    assert gene_reference.covers_release(datetime.datetime(2026, 10, 19, tzinfo=utc), release)
    assert not gene_reference.covers_release(datetime.datetime(2026, 10, 10, 23, 59, tzinfo=utc), release)
    assert not gene_reference.covers_release(None, release)
