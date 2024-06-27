import dataclasses
from typing import List

from clinvar_ingest.model.common import Model


@dataclasses.dataclass
class RcvMapping(Model):
    rcv_accession: str
    scv_accession: str
    trait_set_id: str

    scv_trait_set: dict
    rcv_trait_set: dict

    @staticmethod
    def jsonifiable_fields() -> List[str]:
        return ["scv_trait_set", "rcv_trait_set"]

    def __post_init__(self):
        self.entity_type = "rcv_mapping"

    @staticmethod
    def from_xml(inp: dict):
        """
        Accepts a ClinVarSet XML node and returns a RcvScvMap object
        """
        rcv = inp["ReferenceClinVarAssertion"]
        rcv_accession = rcv["ClinVarAccession"]["@Acc"]
        rcv_trait_set = rcv["TraitSet"]
        scv = inp["ClinVarAssertion"]
        scv_accession = scv["ClinVarAccession"]["@Acc"]
        scv_trait_set = scv["TraitSet"]

        return RcvMapping(
            rcv_accession=rcv_accession,
            scv_accession=scv_accession,
            trait_set_id=rcv_trait_set["@ID"],
            rcv_trait_set=rcv_trait_set,
            scv_trait_set=scv_trait_set,
        )

    def disassemble(self):
        yield self
