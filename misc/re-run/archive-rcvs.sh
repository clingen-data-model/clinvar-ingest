#!/bin/bash
set -xeo pipefail
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_07_24_v1_1_0_beta4/clinvar_xml/ClinVarRCVRelease_2024-0724.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0724.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_07_30_v1_1_0_beta4/clinvar_xml/ClinVarRCVRelease_2024-0730.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0730.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_08_05_v1_1_0_beta4/clinvar_xml/ClinVarRCVRelease_2024-0805.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0805.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_08_12_v1_1_0_beta5/clinvar_xml/ClinVarRCVRelease_2024-0812.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0812.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_08_18_v1_1_0_beta5/clinvar_xml/ClinVarRCVRelease_2024-0818.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0818.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_08_25_v1_1_0_beta5/clinvar_xml/ClinVarRCVRelease_2024-0825.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0825.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_09_02_v1_1_0_beta6/clinvar_xml/ClinVarRCVRelease_2024-0902.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0902.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_09_08_v1_1_0_beta6/clinvar_xml/ClinVarRCVRelease_2024-0908.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0908.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_09_17_v1_1_0_beta6/clinvar_xml/ClinVarRCVRelease_2024-0917.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-0917.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_10_01_v1_0_0/clinvar_xml/ClinVarRCVRelease_2024-1001.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1001.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_10_09_v1_0_0/clinvar_xml/ClinVarRCVRelease_2024-1009.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1009.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_10_14_v1_0_0/clinvar_xml/ClinVarRCVRelease_2024-1014.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1014.xml.gz

# new ones since that scrape
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_10_20_v1_0_0/clinvar_xml/ClinVarRCVRelease_2024-1020.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1020.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_10_27_v2_0_0_alpha/clinvar_xml/ClinVarRCVRelease_2024-1027.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1027.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_11_03_v2_0_0_alpha/clinvar_xml/ClinVarRCVRelease_2024-1103.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1103.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_11_11_v2_0_0_alpha/clinvar_xml/ClinVarRCVRelease_2024-1111.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1111.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_11_20_v2_0_1_alpha/clinvar_xml/ClinVarRCVRelease_2024-1120.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1120.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_11_26_v2_0_1_alpha/clinvar_xml/ClinVarRCVRelease_2024-1126.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1126.xml.gz
gcloud storage cp \
    gs://clinvar-ingest-dev/executions/clinvar_rcv_2024_12_01_v2_0_1_alpha/clinvar_xml/ClinVarRCVRelease_2024-1201.xml.gz \
    gs://clinvar-ingest-dev/xml_archives/rcv/ClinVarRCVRelease_2024-1201.xml.gz
