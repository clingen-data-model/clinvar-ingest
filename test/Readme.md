# ClinVar Ingest - Test Data Sets

These two sets of test cases cover both the Original ClinVarVariationRelease\*\.xml and the new format that allows for multiple Interpretation/Classification types found [here](https://github.com/ncbi/clinvar/tree/master).

Each set includes several single variation XML files that cover various features needed to exercise a scope of items that will be needed in te full releases. Additionally, a single \*TestDataSet.xml.gz file containing all of the selected variation test cases is also available for test processing.

## Original XML Test Data Set (Interpretation = GermlineClassification for all)
The original weekly ClinVarVariationRelease\_\*.XML files (see [here](https://ftp.ncbi.nlm.nih.gov/pub/clinvar/xml/clinvar_variation/weekly_release/)) like the new VCV\_\*.XML files (see [here](https://github.com/ncbi/clinvar/blob/master/sample_xmls/vcv_01.xml)) uses the VCV accession as the top level element. Each VCV or VariationArchive element contains everything that a single VCV would contain. 

The comprehensive [OriginalTestDataSet.xml.gz](data/OriginalTestDataSet.xml.gz) file is a single compressed version containing all of the Original XML test cases below.

1. Basic - InDel VarPath VCV ( [original-clinvar-variation-2.xml](data/original-clinvar-variation-2.xml) )
    - Submitters: 2
    - Submissions: 2 (1 needs generation)
    - Genes: 1 
    - Traits: 1 
    - SCVs/ClinicalAssertions: 2 (1 0-star & 1 1-star)
    - RCVs/TraitSets: 1
    - VCV/VariationArchive: Pathogenic (Germline)

2. Volumonous - SNV conflicting VarPath  ( [original-clinvar-variation-10.xml](data/original-clinvar-variation-10.xml) )
    - Submitters: 42
    - Submissions: 42 
    - Genes: 2 
    - Traits: 14 (includes not provided, not specified and see cases amongst others)
    - SCVs/ClinicalAssertions: 31 1-star (P;P,lp;VUS;B;other), 8 0-star (P;risk factor), 3 no assertion provided 
    - RCVs/TraitSets: 11  (single, multiple and not provided trait traitSets)
    - VCVs/VariationArchives: Conflicting interpretations of pathogenicity; other (Germline)
    - Also includes DeletedSCV section as well as ReplacedList in one or more SCVs

3. IncludedRecord - SNV that has no SCVs ( [clinvar-variation-1264328.xml](data/clinvar-variation-1264328.xml) )
    - included in Genotype/Haplotype but no direct submissions
    - SubmittedInterpretationList (SCVs that depend on this variation)
    - InterpretedVariationList (VCVs that depend on this variation)

4. Haplotype - Pathogenic Expert Review (3-star) ( [original-clinvar-variation-40200.xml](data/original-clinvar-variation-40200.xml) ) 
    - Submitters: 3
    - Submissions: 3
    - Genes: 1 
    - Traits: 1 
    - SCVs/ClinicalAssertions: 1 3-star (P), 1 1-star (P), 1 0-star (P)
    - RCVs/TraitSets: 1  
    - VCVs/VariationArchives: Pathogenic (Germline)

5. Genotype - Drug response practice guideline (4-star) ( [original-clinvar-variation-634266.xml](original-clinvar-variation-634266.xml) )
    - Submitters: 1
    - Submissions: 4
    - Genes: 1 
    - Traits: 4 Type=DrugResponse
    - SCVs/ClinicalAssertions: 4 (all 4-star)
    - RCVs/TraitSets: 4
    - VCV/VariationArchive: Drug Response (Germline)

## New XML Test Data Set (Germline | SomaticImpact | Oncogenicity Classifications)

   - TBD