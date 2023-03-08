import subprocess, os, json
from azure.storage.blob import BlobServiceClient
from azure.servicebus import ServiceBusClient, AutoLockRenewer
from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

from azure.keyvault.secrets import SecretClient
from azure.identity import DefaultAzureCredential

keyVaultName = "encodingkv"
KVUri = f"https://{keyVaultName}.vault.azure.net"

credential = DefaultAzureCredential()
client = SecretClient(vault_url=KVUri, credential=credential)

#Aure variables

connect_str_input = client.get_secret("connectStrInput")
connect_str_output = client.get_secret("connectStrOutput")
connect_str_servicebus = client.get_secret("connectStrServicebus")
queue_name = "main"

output_container_name = "main"

#Local variables

upload_file_path = "output.mp4"
download_file_path = "input.mp4"


def downloadBlob(connect_str, container_name, blob_name, local_file_path):

    blob_service_client = BlobServiceClient.from_connection_string(connect_str.value)

    try:

        print("Downloading input file: " + blob_name)
        blob = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(local_file_path, "wb") as download_file:
            download_file.write(blob.download_blob().readall())

    except Exception as ex:
        print("Exception:")
        print(ex)


def uploadBlob(connect_str, container_name, blob_name, local_file_path):

    blob_service_client = BlobServiceClient.from_connection_string(connect_str.value)

    try:

        print("Uploading output file: " + blob_name)
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data)

    except Exception as ex:
        print("Exception:")
        print(ex)

    os.remove(local_file_path)


def runFfmpeg(input_file_path, output_file_path):
    try:

        print("Encoding input file: ")
        subprocess.run(["ffmpeg", "-i", input_file_path, "-c:v", "libx265", output_file_path])

    except Exception as ex:
        print("Exception:")
        print(ex)
        
    os.remove(input_file_path)


def getBlobFromEventGrid(event):
    try:
        json_event = json.loads(str(event))
        blob_path = json_event["subject"]
        blob_path_tail = os.path.split(blob_path)
        
        temp_path = os.path.split(blob_path_tail[0])
        container_path = os.path.split(temp_path[0])
        container_path_tail = container_path[1]
        
        blob_path_tail = blob_path_tail[1]

        return container_path_tail, blob_path_tail

    except Exception as ex:
        print("Exception:")
        print(ex)

while True:
    print("Waiting for messages...")
    renewer = AutoLockRenewer()
    with ServiceBusClient.from_connection_string(connect_str_servicebus.value) as client:
        with client.get_queue_receiver(queue_name) as receiver:
            for msg in receiver.receive_messages():
                renewer.register(receiver, msg, max_lock_renewal_duration=86400)

                input_container_name, input_blob_name = getBlobFromEventGrid(msg)

                output_blob_name  = "encoded_" + input_blob_name

                downloadBlob(connect_str_input, input_container_name, input_blob_name, download_file_path)
                runFfmpeg(download_file_path, upload_file_path)
                uploadBlob(connect_str_output, output_container_name, output_blob_name, upload_file_path)

                receiver.complete_message(msg)
    renewer.close()