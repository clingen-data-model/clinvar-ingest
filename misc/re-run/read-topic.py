"""
Read a topic start to finish and write to a file, with messages delimited by newlines.
Example:
python misc/re-run/read-topic.py \
    ./kafka-prod.properties \
    clinvar-somatic-ftp-watcher \
    clinvar-somatic-ftp-watcher.txt


python misc/re-run/read-topic.py \
    ./kafka-prod.properties \
    clinvar-rcv-ftp-watcher \
    confluent-prod_clinvar-rcv-ftp-watcher_20241203.txt
"""

import argparse
import sys

from confluent_kafka import Consumer, KafkaError


def load_properties(file_path):
    """
    Load key-value pairs from a .properties file (no section headers).
    """
    properties = {}
    with open(file_path, "r") as file:
        for line in file:
            # Ignore comments and empty lines
            line = line.strip()
            if line and not line.startswith("#"):
                key, value = line.split("=", 1)
                properties[key.strip()] = value.strip()
    return properties


def on_assign(consumer, partitions):
    """
    Rebalance callback to handle partition assignment and seek to the beginning.
    """
    print(f"Partitions assigned: {partitions}")
    for partition in partitions:
        partition.offset = 0  # Set the offset to the beginning
    print("Seeking to the beginning of all newly assigned partitions.")
    consumer.assign(partitions)


def consume_kafka_messages(kafka_config, topic, output_file):
    """
    Consume messages from a Kafka topic and write them to a file.
    """
    # Create a Kafka consumer
    consumer = Consumer(kafka_config)
    consumer.subscribe([topic], on_assign=on_assign)
    print(f"Subscribed to topic: {topic}")

    try:
        with open(output_file, "w") as file:
            while True:
                # Poll for a message
                msg = consumer.poll(1.0)  # 1-second timeout
                print(f"Got message: {msg}")
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        print(f"Reached end of partition for {msg.topic()}")
                        break
                    elif msg.error():
                        raise KafkaError(msg.error())
                else:
                    print(msg.value())
                    print(f"{len(msg.value())}")
                    # Write the message to the file
                    file.write(msg.value().decode("utf-8") + "\n")
                    print(f"Message written: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        print("Consumption interrupted by user.")
    finally:
        consumer.close()


def parse_args(argv):
    """
    Parse command line arguments.
    """
    parser = argparse.ArgumentParser(description="Consume messages from a Kafka topic.")
    parser.add_argument(
        "kafka_properties_file", help="Path to the Kafka properties file."
    )
    parser.add_argument("topic", help="Kafka topic to consume messages from.")
    parser.add_argument("output_file", help="Output file to write messages to.")
    return parser.parse_args(argv)


if __name__ == "__main__":
    # Parse command-line arguments
    args = parse_args(sys.argv[1:])

    # Load Kafka properties
    kafka_properties = load_properties(args.kafka_properties_file)

    # Consume messages and write them to a file
    consume_kafka_messages(kafka_properties, args.topic, args.output_file)
