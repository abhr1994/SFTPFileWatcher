"""
This code monitors for any file created/modified events and sends them to azure storage queue
"""
from azure.storage.queue import QueueServiceClient
import queue
import subprocess
import sys
import time
from threading import Thread
import watchdog.events
import watchdog.observers
import os

num_fetch_threads = 2
job_queue = queue.Queue(maxsize=100)

connect_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
queue_name = os.getenv("AZURE_STORAGE_QUEUE_NAME")
account_access_key = os.getenv("AZURE_STORAGE_ACCOUNT_ACCESS_KEY")
account_url = os.getenv("AZURE_STORAGE_ACCOUNT_URL")
queue_service = QueueServiceClient(account_url=account_url,
                                   credential=account_access_key)
queue_client = queue_service.get_queue_client(queue=queue_name)


def write_message_to_queue(message):
    # queue_service.encode_function = QueueMessageFormat.binary_base64encode
    # queue_service.decode_function = QueueMessageFormat.binary_base64decode
    print("Adding message: " + message)
    queue_client.send_message(message)


def check_if_file_is_complete(i, q):
    while True:
        # print('%s: Looking for the next task ' % i)
        path = q.get()
        command = ["lsof", path]
        lsof = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        try:
            output, errs = lsof.communicate(timeout=20)
            if output.decode('utf-8') != "":
                print("File is still open. Please wait before sending event to the storage queue")
                print(output.decode('utf-8'))
            else:
                print("Ready to push the event to the storage queue")
                message = f"{path} is ready to be ingested"
                write_message_to_queue(message)
        except Exception as e:
            lsof.kill()
            # output, errs = proc.communicate()
        q.task_done()


class Handler(watchdog.events.PatternMatchingEventHandler):
    def __init__(self):
        # Set the patterns for PatternMatchingEventHandler
        watchdog.events.PatternMatchingEventHandler.__init__(
            self,
            patterns=["*.DAT"],
            ignore_directories=True,
            case_sensitive=False,
        )

    def on_any_event(self, event):
        print(
            "[{}] noticed: [{}] on: [{}] ".format(
                time.asctime(), event.event_type, event.src_path
            )
        )
        if event.event_type in ["modified", "created"]:
            job_queue.put(event.src_path)


if __name__ == "__main__":
    path = sys.argv[1] if len(sys.argv) > 1 else "/tmp/cfs/app"
    event_handler = Handler()
    observer = watchdog.observers.Observer()
    observer.schedule(event_handler, path=path, recursive=True)
    observer.start()
    for i in range(num_fetch_threads):
        worker = Thread(target=check_if_file_is_complete, args=(i, job_queue,))
        worker.setDaemon(True)
        worker.start()
    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

    print('*** Main thread waiting ***')
    job_queue.join()
    print('*** Done ***')
