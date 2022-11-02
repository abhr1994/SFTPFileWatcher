"""
This code reads the messages from azure storage queue and triggers the ingestion for corresponding table
"""
from azure.storage.queue import QueueServiceClient
import csv
import re
import os

account_access_key = os.getenv("AZURE_STORAGE_ACCOUNT_ACCESS_KEY")
account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
queue_service = QueueServiceClient(account_url=account_url, credential=account_access_key)
queue_name = os.getenv("AZURE_STORAGE_QUEUE_NAME")
queue_client = queue_service.get_queue_client(queue=queue_name)

reference_dict = {}
with open("table_details.csv", 'r') as file:
    csv_file = csv.DictReader(file)
    for row in csv_file:
        temp = dict(row)
        reference_dict[temp["table_name"]] = {"file_path": temp["file_path"], "regex_pattern": temp["regex_pattern"]}

response = queue_client.receive_messages()
for message in response:
    for key in reference_dict:
        regex_code = reference_dict[key]["regex_pattern"]
        path = message.content.split("is ready to be ingested")[0].strip()
        file_name = path.split("/")[-1]
        parent = os.path.dirname(path)
        if bool(re.match(regex_code, file_name)) and parent == reference_dict[key]["file_path"]:
            print(f"Trigger ingestion for table {key}")
            # Write code here to trigger Table Ingestion
            break

    queue_client.delete_message(message)
