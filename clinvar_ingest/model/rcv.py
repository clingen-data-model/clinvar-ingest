import dataclasses

from clinvar_ingest.model.common import Model
from clinvar_ingest.utils import ensure_list


@dataclasses.dataclass
class RcvMapping(Model):
    """
    Represents a RCV -(1..N)-> SCV Mapping parsed from ClinVar's RCV XML format.
    One RCV can be associated with multiple SCVs.
    """

    rcv_accession: str
    scv_accessions: list[str]
    trait_set_id: str
    trait_set_content: dict

    @staticmethod
    def jsonifiable_fields() -> list[str]:
        return ["trait_set_content"]

    def __post_init__(self):
        self.entity_type = "rcv_mapping"

    @staticmethod
    def from_xml(inp: dict):
        """
        Accepts a ClinVarSet XML node and returns a RcvMapping object
        """
        rcv = inp["ReferenceClinVarAssertion"]
        rcv_accession = rcv["ClinVarAccession"]["@Acc"]
        rcv_trait_set = rcv["TraitSet"]
        scvs = ensure_list(inp["ClinVarAssertion"])
        scv_accessions = [scv["ClinVarAccession"]["@Acc"] for scv in scvs]

        return RcvMapping(
            rcv_accession=rcv_accession,
            scv_accessions=scv_accessions,
            trait_set_id=rcv_trait_set["@ID"],
            trait_set_content=rcv_trait_set,
        )

    def disassemble(self):
        yield self
