"""
Translate one variation_identity record to its VRS object.

Only genomic expressions are translated: SPDI, gnomAD, and HGVS on a genomic (g.) or
mitochondrial (m.) reference. Transcript and protein expressions get an errors row.
"""

import re
from typing import Any

TRANSLATED_CLASSES = ("Allele", "CopyNumberChange", "CopyNumberCount")

# variation_identity assembly_version -> RefgetStore assembly alias namespace
ASSEMBLY_NAMES = {"38": "GRCh38", "37": "GRCh37", "36": "NCBI36"}
DEFAULT_ASSEMBLY_VERSION = "38"

# copy_change_type or variation_type -> VRS copyChange
COPY_CHANGES = {
    "loss": "loss",
    "deletion": "loss",
    "copy number loss": "loss",
    "gain": "gain",
    "duplication": "gain",
    "copy number gain": "gain",
}

_GENOMIC_HGVS = re.compile(r"^[^:]+:[gm]\.")


def is_genomic(fmt: str | None, source: str) -> bool:
    return fmt in ("spdi", "gnomad") or (fmt == "hgvs" and bool(_GENOMIC_HGVS.match(source)))


def _translate_allele(record: dict, allele_translators: dict[str, Any]) -> Any:
    assembly_version = record.get("assembly_version") or DEFAULT_ASSEMBLY_VERSION
    translator = allele_translators.get(assembly_version)
    if translator is None:
        raise ValueError(f"unknown assembly_version {assembly_version!r}")
    return translator.translate_from(record["source"], fmt=record["fmt"])


def _translate_copy_number(record: dict, cnv_translator: Any) -> Any:
    if record["vrs_class"] == "CopyNumberCount":
        return cnv_translator.translate_from(record["source"], fmt="hgvs", copies=int(record["absolute_copies"]))
    kind = (record.get("copy_change_type") or record.get("variation_type") or "").lower()
    copy_change = COPY_CHANGES.get(kind)
    if copy_change is None:
        raise ValueError(f"unknown copy change {kind!r}")
    return cnv_translator.translate_from(record["source"], fmt="hgvs", copy_change=copy_change)


def translate_record(record: dict, allele_translators: dict[str, Any], cnv_translator: Any) -> dict | None:
    """
    Return the `out` value for a variation_identity record: the VRS object as a dict
    without its location's sequence, an `{"errors": ...}` dict, or None for classes with
    no expression to translate.

    `allele_translators` maps assembly_version ("38", "37", "36") to an AlleleTranslator
    whose default assembly is that version.
    """
    vrs_class = record.get("vrs_class")
    source = record.get("source")
    if vrs_class not in TRANSLATED_CLASSES or not source:
        return None
    if not is_genomic(record.get("fmt"), source):
        return {"errors": f"not translated: non-genomic expression: {source}"}

    try:
        if vrs_class == "Allele":
            variation = _translate_allele(record, allele_translators)
        else:
            variation = _translate_copy_number(record, cnv_translator)
    except Exception as e:  # noqa: BLE001 - every failure, including a timeout, becomes an errors row
        return {"errors": f"{type(e).__name__}: {e}"}

    if variation is None:
        return {"errors": f"not translated: {source}"}
    out = variation.model_dump(exclude_none=True)
    out.get("location", {}).pop("sequence", None)
    return out
