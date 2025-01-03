import json

# actual_archives_file = "vcv-archives.txt"
ftp_watcher_file = "confluent-prod_clinvar-somatic-ftp-watcher_20241203.txt"
output_ftp_watcher_file = "updated_clinvar-somatic-ftp-watcher_20241203.txt"

with open(ftp_watcher_file) as f:
    ftp_messages = f.readlines()
    ftp_messages = [json.loads(msg) for msg in ftp_messages]

# with open(actual_archives_file) as f:
#     actual_archives = f.readlines()

output_records = []

for ftp_message in ftp_messages:
    for msg in ftp_message:
        msg["Host"] = "gs://clinvar-ingest-dev"
        msg["Directory"] = "/xml_archives/vcv"

with open(output_ftp_watcher_file, "w") as f:
    for msg in ftp_messages:
        f.write(json.dumps(msg) + "\n")
