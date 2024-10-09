"""
This module is meant to be used as a CLI to read a ClinVarVariationRelease XML
file and write a smaller number of VariationArchive objects to output files.

This filters a clinvar XML file for any VariationArchive elements that have
all three classification statement types:
GermlineClassification, SomaticClinicalImpact, and OncogenicityClassification

Example:
python test/data/filter_somatic-oncogenicity.py \
    -i clinvar-new/ClinVarVCVRelease_2024-08.xml.gz


"""

import argparse
import gzip
import os
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

from clinvar_ingest.model.variation_archive import StatementType


def parse_args(argv) -> dict:
    """
    Parses options: --input-filename (-i), --output-filename (-o) and --max-count (-m)
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-filename", "-i", required=True, type=str)
    parser.add_argument("--output-directory", "-o", required=True, type=str)
    parser.add_argument("--max-count", "-m", required=False, type=int, default=10)
    parsed = vars(parser.parse_args(argv))

    # Make sure the output directory exists or can be created
    if not os.path.exists(parsed["output_directory"]):
        os.makedirs(parsed["output_directory"])

    return parsed


def get_classifications(variation_archive_elem: ET.Element) -> list[ET.Element]:
    """
    Get each classification element from the Classifications element of a VariationArchive element.
    Includes only those in ClassifiedRecords, not IncludedRecords, since IncludedRecord
    adds empty placeholders for missing classifications.
    """

    classifications = variation_archive_elem.find("ClassifiedRecord/Classifications")
    # if classifications is None:
    #     classifications = variation_archive_elem.find("IncludedRecord/Classifications")
    if classifications is None:
        return []
    types = set(o.value for o in StatementType)
    stmts = []
    for statement_key in types:
        statement = classifications.find(statement_key)
        if statement is not None:
            stmts.append(statement)
    return stmts


def main(opts):
    """
    Main entrypoint for CLI.
    """
    with gzip.open(opts["input_filename"], "rt") as f_in:
        # Use ElementTree to iterate over f_in, and when a VariationArchive meets
        # the criteria write it to a file.
        opening_tag = None
        closing_tag = "</ClinVarVariationRelease>"
        item_count = 0
        item_limit = opts["max_count"]
        for event, elem in ET.iterparse(f_in, events=["start", "end"]):
            if event == "end" and elem.tag == "VariationArchive":
                # Check if the VCV accession is in the filter list
                accession = elem.attrib["Accession"]

                # Get the Classifications
                try:
                    classifications = get_classifications(elem)
                except Exception as e:
                    print(f"Error getting classifications: {e}")
                    continue

                if len(classifications) == 3:
                    print(f"Accession: {accession}")
                    print(f"Found all classifications: {classifications}")

                    #     # Open file, write opening tag, write element, write closing tag
                    assert opening_tag
                    elem_s = ET.tostring(elem)
                    filename = Path(opts["output_directory"]) / (accession + ".xml")
                    with open(filename, "wb") as f_out:
                        f_out.write(opening_tag)
                        f_out.write(b"\n")
                        f_out.write(elem_s.rstrip())
                        f_out.write(b"\n")
                        f_out.write(closing_tag.encode("utf-8"))
                        f_out.write(b"\n")

                    item_count += 1
                    if item_count >= item_limit:
                        print(f"Reached max count: {item_count} >= {item_limit}")
                        break

                # Clear completed VariationArchive element
                elem.clear()

            elif event == "start" and elem.tag == "ClinVarVariationRelease":
                opening_tag = ET.tostring(elem).split(b"\n")[0]

            elif event == "end" and elem.tag == "ClinVarVariationRelease":
                break


if __name__ == "__main__":
    argv = sys.argv[1:]
    if len(argv) == 0:
        argv = [
            "--input-filename",
            "data/ClinVarVCVRelease_2024-0917.xml.gz",
            "--output-directory",
            "complex-vcv-classifications",
        ]
    options = parse_args(argv)
    sys.exit(main(options))
