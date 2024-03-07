from __future__ import annotations

import dataclasses
import json
import logging
from typing import List

from clinvar_ingest.model.common import Model, dictify, model_copy
from clinvar_ingest.utils import ensure_list, extract, flatten1, get

_logger = logging.getLogger("clinvar_ingest")


def extract_element_xrefs(
    attr: dict, ref_field: str, ref_field_element: str = None
) -> List[Trait.XRef]:
    """
    Extract XRefs from an element, with the option to specify a ref_field and ref_field_element
    where it came from (used to differentiate xrefs within different parent elements).
    """
    outputs = []
    for x in ensure_list(attr.get("XRef", [])):
        outputs.append(
            Trait.XRef(
                db=x["@DB"],
                id=x["@ID"],
                type=get(x, "@Type"),
                ref_field=ref_field,
                ref_field_element=ref_field_element,
            )
        )
    return outputs


@dataclasses.dataclass
class TraitMetadata(Model):
    """
    This class is used to parse the shared fields between Trait and ClinicalAssertionTrait.

    Top-level xrefs are included, plus xrefs which appear within the Name elements.
    """

    id: str
    type: str
    name: str
    medgen_id: str
    alternate_names: List[str]
    xrefs: List[Trait.XRef]

    # content: dict

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.info(f"TraitMetadata.from_xml(inp={json.dumps(inp)})")

        id = extract(inp, "@ID")
        trait_type = extract(inp, "@Type")
        # Preferred Name (Name type=Preferred)
        names = ensure_list(extract(inp, "Name") or [])
        preferred_names = [
            n for n in names if get(n, "ElementValue", "@Type") == "Preferred"
        ]
        if len(preferred_names) > 1:
            raise RuntimeError(f"Trait {id} has multiple preferred names")
        preferred_name = None
        if len(preferred_names) == 1:
            preferred_name = preferred_names[0]["ElementValue"]["$"]

        preferred_name_xrefs = [
            extract_element_xrefs(n, "name", n["ElementValue"]["$"])
            for n in preferred_names
        ]
        _logger.debug("preferred_name: %s", preferred_name)

        # Alternate Names (Name type=Alternate)
        alternate_names = [
            n for n in names if get(n, "ElementValue", "@Type") == "Alternate"
        ]
        alternate_name_strs = [get(n, "ElementValue", "$") for n in alternate_names]

        alternate_name_xrefs = [
            extract_element_xrefs(n, "alternate_names", n["ElementValue"]["$"])
            for n in alternate_names
        ]
        _logger.debug("alternate_names: %s", json.dumps(alternate_name_strs))

        # XRefs which are at the top level of Trait objects in the XML
        top_xrefs = extract_element_xrefs(inp, ref_field=None, ref_field_element=None)
        _logger.debug("top_xrefs: %s", json.dumps(dictify(top_xrefs)))

        # Try to get a MedGen ID from the only the top level XRefs
        medgen_id = None
        _medgen_xrefs = [x for x in top_xrefs if x.db == "MedGen"]
        if len(_medgen_xrefs) > 1:
            raise RuntimeError(
                f"Trait {id} has multiple MedGen XRefs: {[m.id for m in _medgen_xrefs]}"
            )
        if len(_medgen_xrefs) == 1:
            medgen_id = _medgen_xrefs[0].id

        obj = TraitMetadata(
            id=id,
            type=trait_type,
            name=preferred_name,
            medgen_id=medgen_id,
            alternate_names=alternate_name_strs,
            xrefs=top_xrefs + preferred_name_xrefs + alternate_name_xrefs,
        )
        return obj

    def disassemble(self):
        raise NotImplementedError()


@dataclasses.dataclass
class Trait(Model):
    id: str
    disease_mechanism_id: int
    name: str
    attribute_content: List[str]
    mode_of_inheritance: str
    ghr_links: str
    keywords: List[str]
    gard_id: int
    medgen_id: str
    public_definition: str
    type: str
    symbol: str
    disease_mechanism: str
    alternate_symbols: List[str]
    gene_reviews_short: str
    alternate_names: List[str]
    xrefs: List[str]

    content: str

    class XRef:
        def __init__(
            self,
            db: str,
            id: str,
            type: str,
            ref_field: str = None,
            ref_field_element: str = None,
        ):
            """
            ref_field and ref_field_element are used to differentiate between XRefs which may
            apply to similar values but which originate from different nodes in the XML.

            ref_field: the field name which differentiates this XRef
            ref_field_element: the field value which differentiates this XRef

            e.g. ref_field="alternate_symbol", ref_field_element="MC1DN26"
            """
            self.db = db
            self.id = id
            self.type = type
            self.ref_field = ref_field
            self.ref_field_element = ref_field_element

    def __post_init__(self):
        self.entity_type = "trait"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True) -> Trait:
        _logger.debug(f"Trait.from_xml(inp={json.dumps(inp)})")

        trait_metadata = TraitMetadata.from_xml(inp, jsonify_content=jsonify_content)

        # TODO the logic here is the same as Preferred and Alternate Name
        # Preferred Symbol (Symbol type=Preferred)
        symbols = ensure_list(extract(inp, "Symbol") or [])
        preferred_symbols = [
            s for s in symbols if get(s, "ElementValue", "@Type") == "Preferred"
        ]
        if len(preferred_symbols) > 1:
            raise RuntimeError(
                f"Trait {trait_metadata.id} has multiple preferred symbols"
            )
        preferred_symbol = None
        if len(preferred_symbols) == 1:
            preferred_symbol = preferred_symbols[0]["ElementValue"]["$"]

        preferred_symbol_xrefs = [
            extract_element_xrefs(s, "symbol", s["ElementValue"]["$"])
            for s in preferred_symbols
        ]
        _logger.debug(
            "preferred_symbol: %s, preferred_symbol_xrefs: %s",
            preferred_symbol,
            json.dumps(dictify(preferred_symbol_xrefs)),
        )

        # Alternate Symbols (Symbol type=Alternate)
        alternate_symbols = [
            s for s in symbols if get(s, "ElementValue", "@Type") == "Alternate"
        ]
        alternate_symbol_strs = [get(s, "ElementValue", "$") for s in alternate_symbols]

        alternate_symbol_xrefs = [
            extract_element_xrefs(s, "alternate_symbols", s["ElementValue"]["$"])
            for s in alternate_symbols
        ]
        _logger.debug(
            "alternate_symbols: %s, alternate_symbol_xrefs: %s",
            json.dumps(alternate_symbol_strs),
            json.dumps(dictify(alternate_symbol_xrefs)),
        )

        # Get XRefs from nodes inside Trait AttributeSet
        attribute_set = ensure_list(inp.get("AttributeSet", []))

        def pop_attribute(inp_key):
            """
            Looks in AttributeSet for 0..1 attributes with type matching inp_key

            If there are multiple, returns the first. Use pop_attribute_list to get all.
            """
            matching_attributes = [
                a for a in attribute_set if get(a, "Attribute", "@Type") == inp_key
            ]

            if len(matching_attributes) > 0:
                attribute_set.remove(matching_attributes[0])
                return matching_attributes[0]

        def pop_attribute_list(inp_key):
            """
            Looks in AttributeSet for 0..N attributes with type matching inp_key
            """
            matching_attributes = [
                a for a in attribute_set if get(a, "Attribute", "@Type") == inp_key
            ]
            for a in matching_attributes:
                attribute_set.remove(a)
            return matching_attributes

        # public definition
        public_definition_attr = pop_attribute("public definition")
        if public_definition_attr is not None:
            public_definition = get(public_definition_attr, "Attribute", "$")
            public_definition_xref = extract_element_xrefs(
                public_definition_attr, "public_definition"
            )
        else:
            public_definition = None
            public_definition_xref = None

        # GARD id
        gard_id_attr = pop_attribute("GARD id")
        if gard_id_attr is not None:
            gard_id = int(get(gard_id_attr, "Attribute", "@integerValue"))
            gard_id_xref = extract_element_xrefs(gard_id_attr, "gard_id")
        else:
            gard_id = None
            gard_id_xref = None

        # keyword
        keyword_attrs = pop_attribute_list("keyword")
        if len(keyword_attrs) > 0:
            keywords = [get(a, "Attribute", "$") for a in keyword_attrs]
            keyword_xrefs = [
                extract_element_xrefs(a, "keywords", get(a, "Attribute", "$"))
                for a in keyword_attrs
            ]
        else:
            keywords = None
            keyword_xrefs = []

        # disease mechanism
        disease_mechanism_attr = pop_attribute("disease mechanism")
        if disease_mechanism_attr is not None:
            disease_mechanism = get(disease_mechanism_attr, "Attribute", "$")
            disease_mechanism_id = get(
                disease_mechanism_attr, "Attribute", "@integerValue"
            )
            if disease_mechanism_id is not None:
                disease_mechanism_id = int(disease_mechanism_id)
            disease_mechanism_xref = extract_element_xrefs(
                disease_mechanism_attr, "disease_mechanism"
            )
        else:
            disease_mechanism = None
            disease_mechanism_id = None
            disease_mechanism_xref = None

        # mode of inheritance
        mode_of_inheritance_attr = pop_attribute("mode of inheritance")
        if mode_of_inheritance_attr is not None:
            mode_of_inheritance = get(mode_of_inheritance_attr, "Attribute", "$")
            mode_of_inheritance_xref = extract_element_xrefs(
                mode_of_inheritance_attr, "mode_of_inheritance"
            )
        else:
            mode_of_inheritance = None
            mode_of_inheritance_xref = None

        # GeneReviews short
        gene_reviews_short_attr = pop_attribute("GeneReviews short")
        if gene_reviews_short_attr is not None:
            gene_reviews_short = get(gene_reviews_short_attr, "Attribute", "$")
            gene_reviews_short_xref = extract_element_xrefs(
                gene_reviews_short_attr, "gene_reviews_short"
            )
        else:
            gene_reviews_short = None
            gene_reviews_short_xref = None

        # Genetics Home Reference (GHR) links
        ghr_links_attr = pop_attribute("Genetics Home Reference (GHR) links")
        if ghr_links_attr is not None:
            ghr_links = get(ghr_links_attr, "Attribute", "$")
            ghr_links_xref = extract_element_xrefs(ghr_links_attr, "ghr_links")
        else:
            ghr_links = None
            ghr_links_xref = None

        attribute_set_xrefs = keyword_xrefs + [
            public_definition_xref,
            gard_id_xref,
            disease_mechanism_xref,
            mode_of_inheritance_xref,
            gene_reviews_short_xref,
            ghr_links_xref,
        ]
        _logger.debug(
            "attribute_set_xrefs: %s", json.dumps(dictify(attribute_set_xrefs))
        )

        # Overwrite inp AttributeSet to reflect those popped above
        inp["AttributeSet"] = attribute_set

        all_xrefs = [
            *trait_metadata.xrefs,
            *preferred_symbol_xrefs,
            *alternate_symbol_xrefs,
            *attribute_set_xrefs,
        ]

        # Flatten XRefs
        all_xrefs = flatten1(all_xrefs)
        # Filter out None XRefs
        all_xrefs = [x for x in all_xrefs if x is not None]

        obj = Trait(
            id=trait_metadata.id,
            type=trait_metadata.type,
            name=trait_metadata.name,
            alternate_names=trait_metadata.alternate_names,
            symbol=preferred_symbol,
            alternate_symbols=alternate_symbol_strs,
            mode_of_inheritance=mode_of_inheritance,
            ghr_links=ghr_links,
            keywords=keywords,
            gard_id=gard_id,
            medgen_id=trait_metadata.medgen_id,
            public_definition=public_definition,
            disease_mechanism=disease_mechanism,
            disease_mechanism_id=disease_mechanism_id,
            gene_reviews_short=gene_reviews_short,
            xrefs=all_xrefs,
            attribute_content=attribute_set,
            content=inp,
        )
        if jsonify_content:
            obj.content = json.dumps(inp)
            obj.attribute_content = [json.dumps(a) for a in attribute_set]
            obj.xrefs = [json.dumps(dictify(x)) for x in all_xrefs]
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class TraitSet(Model):
    id: str
    type: str
    traits: List[Trait]

    content: str

    def __post_init__(self):
        self.trait_ids = [t.id for t in self.traits]
        self.entity_type = "trait_set"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.debug(f"TraitSet.from_xml(inp={json.dumps(dictify(inp))})")
        obj = TraitSet(
            id=extract(inp, "@ID"),
            type=extract(inp, "@Type"),
            traits=[
                Trait.from_xml(t, jsonify_content=jsonify_content)
                for t in ensure_list(extract(inp, "Trait"))
            ],
            content=inp,
        )
        if jsonify_content:
            obj.content = json.dumps(inp)
            for t in obj.traits:
                t.content = json.dumps(t.content)
        return obj

    def disassemble(self):
        for t in self.traits:
            for val in t.disassemble():
                yield val
        del self.traits
        yield self


@dataclasses.dataclass
class ClinicalAssertionTrait(Model):
    id: str
    type: str
    name: str
    medgen_id: str
    trait_id: str
    alternate_names: List[str]
    xrefs: List[Trait.XRef]

    content: dict

    def __post_init__(self):
        self.entity_type = "clinical_assertion_trait"

    @staticmethod
    def match_to_trait(me: Trait, normalized_traits: List[Trait]):
        """
        Given a list of normalized traits, find the one that matches the clinical assertion trait

        Tries to find a match through these conditions, in order:
        - me.medgen_id = t.medgen_id
        - one of me.xrefs equals one of t.xrefs
        - trait_mapping field matches between me and t
          - name
          - preferred_name
          - alternate_name
          - trait mapping xref

        """
        # TODO match submitted traits to normalized traits
        return None

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True, normalized_traits: List[Trait] = []):
        _logger.debug(
            f"ClinicalAssertionTrait.from_xml(inp={json.dumps(dictify(inp))})"
        )

        trait_metadata = TraitMetadata.from_xml(inp, jsonify_content=jsonify_content)

        # Map submitted trait to normalized trait
        trait_id = ClinicalAssertionTrait.match_to_trait(
            trait_metadata, normalized_traits
        )

        obj = ClinicalAssertionTrait(
            id=trait_metadata.id,
            type=trait_metadata.type,
            name=trait_metadata.name,
            medgen_id=trait_metadata.medgen_id,
            trait_id=trait_id,
            alternate_names=trait_metadata.alternate_names,
            xrefs=trait_metadata.xrefs,
            content=inp,
        )
        if jsonify_content:
            obj.content = json.dumps(obj.content)
            obj.xrefs = [json.dumps(dictify(x)) for x in obj.xrefs]
        return obj

    def disassemble(self):
        yield self


@dataclasses.dataclass
class ClinicalAssertionTraitSet(Model):
    """
    This class is identical to TraitSet except

    - entity_type = "clinical_assertion_trait_set"
    - trait_ids is renamed to clinical_assertion_trait_ids
    """

    id: str
    type: str
    traits: List[ClinicalAssertionTrait]
    content: dict

    def __post_init__(self):
        self.entity_type = "clinical_assertion_trait_set"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        _logger.debug(
            f"ClinicalAssertionTraitSet.from_xml(inp={json.dumps(dictify(inp))})"
        )
        obj = ClinicalAssertionTraitSet(
            id=extract(inp, "@ID"),
            type=extract(inp, "@Type"),
            traits=[
                ClinicalAssertionTrait.from_xml(t, jsonify_content=jsonify_content)
                for t in ensure_list(extract(inp, "Trait"))
            ],
            content=inp,
        )
        if jsonify_content:
            obj.content = json.dumps(obj.content)
        return obj

    def disassemble(self):
        self_copy = model_copy(self)
        for t in self_copy.traits:
            for val in t.disassemble():
                yield val
        trait_ids = [t.id for t in self_copy.traits]
        del self_copy.traits
        setattr(self_copy, "clinical_assertion_trait_ids", trait_ids)
        yield self_copy


@dataclasses.dataclass
class TraitMapping(Model):
    clinical_assertion_id: str
    trait_type: str
    mapping_type: str
    mapping_value: str
    mapping_ref: str
    medgen_name: str
    medgen_id: str

    def __post_init__(self):
        self.entity_type = "trait_mapping"

    @staticmethod
    def from_xml(inp: dict, jsonify_content=True):
        return TraitMapping(
            clinical_assertion_id=extract(inp, "@ClinicalAssertionID"),
            trait_type=extract(inp, "@TraitType"),
            mapping_type=extract(inp, "@MappingType"),
            mapping_value=extract(inp, "@MappingValue"),
            mapping_ref=extract(inp, "@MappingRef"),
            medgen_name=extract(inp, "MedGen", "@Name"),
            medgen_id=extract(inp, "MedGen", "@CUI"),
        )

    def disassemble(self):
        yield self
