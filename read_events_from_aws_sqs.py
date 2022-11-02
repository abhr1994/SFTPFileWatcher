"""
This code reads the messages from aws sqs queue and triggers the ingestion for corresponding table
"""
import boto3
import csv
import re
import os

sqs_client = boto3.client('sqs', endpoint_url='http://localhost:4566')
queue_url = os.getenv("SQS_QUEUE_URL")

reference_dict = {}
with open("table_details.csv", 'r') as file:
    csv_file = csv.DictReader(file)
    for row in csv_file:
        temp = dict(row)
        reference_dict[temp["table_name"]] = {"file_path": temp["file_path"], "regex_pattern": temp["regex_pattern"]}


def get_messages_from_queue(queue_url):
    messages = []
    while True:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            AttributeNames=['All'],
            MaxNumberOfMessages=10
        )
        try:
            messages.extend(resp['Messages'])
        except KeyError:
            break
        entries = [
            {'Id': msg['MessageId'], 'ReceiptHandle': msg['ReceiptHandle']}
            for msg in resp['Messages']
        ]
        resp = sqs_client.delete_message_batch(
            QueueUrl=queue_url, Entries=entries
        )
        if len(resp['Successful']) != len(entries):
            raise RuntimeError(
                f"Failed to delete messages: entries={entries!r} resp={resp!r}"
            )
    return messages


messages = get_messages_from_queue(queue_url)
for message in messages:
    message_body = message.get("Body", "")
    print(message)
    for key in reference_dict:
        regex_code = reference_dict[key]["regex_pattern"]
        path = message_body.split("is ready to be ingested")[0].strip()
        file_name = path.split("/")[-1]
        parent = os.path.dirname(path)
        if bool(re.match(regex_code, file_name)) and parent == reference_dict[key]["file_path"]:
            print(f"Trigger ingestion for table {key}")
            # Write code here to trigger Table Ingestion
            break
