# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.5'
#       jupytext_version: 1.14.0
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# +
# #!pip install --upgrade google-cloud-documentai
# -

from google.api_core.client_options import ClientOptions
from google.cloud import documentai


# +
# #!pip install cloudstorage[google]

# +
def batch_process_documents_processor_version(
    client,
    project_id: str,
    location: str,
    processor_id: str,
    processor_version_id: str,
    gcs_input_uri: str,
    input_mime_type: str,
    gcs_output_bucket: str,
    gcs_output_uri_prefix: str,
    field_mask: str = None,
    timeout: int = 400,
    
):

    # You must set the api_endpoint if you use a location other than 'us', e.g.:
    #opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

    #client = documentai.DocumentProcessorServiceClient(client_options=opts)

    #gcs_document = documentai.GcsDocument(
    #   gcs_uri=gcs_input_uri, mime_type=input_mime_type
    #)

    # Load GCS Input URI into a List of document files
    #gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
    #input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

    # NOTE: Alternatively, specify a GCS URI Prefix to process an entire directory
    #
    # gcs_input_uri = "gs://bucket/directory/"
    gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_uri)
    input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)
    #

    # Cloud Storage URI for the Output Directory
    destination_uri = f"{gcs_output_bucket}/{gcs_output_uri_prefix}/"

    gcs_output_config = documentai.DocumentOutputConfig.GcsOutputConfig(
        gcs_uri=destination_uri, field_mask=field_mask
    )

    # Where to write results
    output_config = documentai.DocumentOutputConfig(gcs_output_config=gcs_output_config)

    # The full resource name of the processor version
    # e.g. projects/{project_id}/locations/{location}/processors/{processor_id}/processorVersions/{processor_version_id}
    name = client.processor_version_path(
        project_id, location, processor_id, processor_version_id
    )

    request = documentai.BatchProcessRequest(
        name=name,
        input_documents=input_config,
        document_output_config=output_config,
    )

    # BatchProcess returns a Long Running Operation (LRO)
    operation = client.batch_process_documents(request)

    # Continually polls the operation until it is complete.
    # This could take some time for larger files
    # Format: projects/PROJECT_NUMBER/locations/LOCATION/operations/OPERATION_ID
    try:
        print(f"Waiting for operation {operation.operation.name} to complete...")
        operation.result(timeout=timeout)
    # Catch exception when operation doesn't finish before timeout
    except (RetryError) as e:
        print(e.message)

    # NOTE: Can also use callbacks for asynchronous processing
    #
    # def my_callback(future):
    #   result = future.result()
    #
    # operation.add_done_callback(my_callback)

    # Once the operation is complete,
    # get output document information from operation metadata
    metadata = documentai.BatchProcessMetadata(operation.metadata)

    if metadata.state != documentai.BatchProcessMetadata.State.SUCCEEDED:
        raise ValueError(f"Batch Process Failed: {metadata.state_message}")

    storage_client = storage.Client()

    #print("Output files:")
    # One process per Input Document
    for process in metadata.individual_process_statuses:
        # output_gcs_destination format: gs://BUCKET/PREFIX/OPERATION_NUMBER/INPUT_FILE_NUMBER/
        # The Cloud Storage API requires the bucket name and URI prefix separately
        matches = re.match(r"gs://(.*?)/(.*)", process.output_gcs_destination)
        if not matches:
            print(
                "Could not parse output GCS destination:",
                process.output_gcs_destination,
            )
            continue

        output_bucket, output_prefix = matches.groups()

        # Get List of Document Objects from the Output Bucket
        output_blobs = storage_client.list_blobs(output_bucket, prefix=output_prefix)

        # Document AI may output multiple JSON files per source file
        for blob in output_blobs:
            # Document AI should only output JSON files to GCS
            if ".json" not in blob.name:
                print(
                    f"Skipping non-supported file: {blob.name} - Mimetype: {blob.content_type}"
                )
                continue

            # Download JSON File as bytes object and convert to Document Object
            #print(f"Fetching {blob.name}")
            document = documentai.Document.from_json(
               blob.download_as_bytes(), ignore_unknown_fields=True
            )

            # For a full list of Document object attributes, please reference this page:
            # https://cloud.google.com/python/docs/reference/documentai/latest/google.cloud.documentai_v1.types.Document

            # Read the text recognition output from the processor
            print("Jsons created")
            #print(document.text)
            break
        break


# +
import re

from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import RetryError
from google.cloud import documentai, storage

project_id = 'pre-ptpdocmgmt-1059'
location = 'us'
processor_id = '73efe0a727c607ea'
gcs_input_uri= 'gs://pre-ptpdocmgmt-bucket/poc-inputfiles'
#gcs_input_uri= 'gs://pre-ptpdocmgmt-bucket/Single File/ZI300_PAR_0002279373_23220895.PDF'
gcs_output_bucket = 'gs://paramountpoc-exportjson'
gcs_output_uri_prefix= 'poc-testjsons'
#file_path = 'gs://pre-ptpdocmgmt-bucket/Single-test-new/Invoice_23273615_20220621.PDF'
#file_path = 'gs://pre-ptpdocmgmt-bucket/try_test1'
#file_path = 'gs://temp/try_test
mime_type = 'application/pdf'
processor_version_id = '4ccaf04430ed51e5'
field_mask = None
timeout: int = 400
opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
client = documentai.DocumentProcessorServiceClient(client_options=opts)

batch_process_documents_processor_version(client,project_id,location,processor_id,processor_version_id,
                                          gcs_input_uri,mime_type,gcs_output_bucket,gcs_output_uri_prefix,None,timeout)
        
# -


