#!/usr/bin/env python
# coding: utf-8

# In[1]:


#%pip install --upgrade google-cloud-storage


# In[2]:


#%pip install fsspec


# In[3]:


#pip install datalab


# In[22]:


import json
import xml.etree.ElementTree as ET
from google.api_core.client_options import ClientOptions
#from google.cloud import documentai
import glob
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
options = PipelineOptions()
import os
import re
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import RetryError
from google.cloud import documentai, storage

from lxml import etree as ET
from datetime import datetime
import pytz
from datetime import datetime as dt
from google.cloud import storage
import pandas
import io
import google.auth
#from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.options.pipeline_options import SetupOptions

from apache_beam.runners import DataflowRunner


# In[23]:


from google.cloud import storage


def list_blobs(bucket_name):


    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name)
    json_paths = []
    for blob in blobs:

        json_paths.append(f"{blob.name}")

    bucket = storage_client.bucket(bucket_name)
    s=[]
    for i in json_paths:
      blob = bucket.get_blob(i)
      s.append(blob.download_as_string())
    return s


# In[24]:



def list_blobs3(bc):


    """Lists all the blobs in the bucket."""

    storage_client = storage.Client()
    blobs = storage_client.list_blobs('paramountpoc-exportjson', prefix = 'poc-testjsons')
    json_paths = []
    for blob in blobs:
      #json_paths.append(f"gs://{bucket_name}/{blob.name}")
        json_paths.append(f"{blob.name}")
    return json_paths





class list_blobs2(beam.DoFn):

    def setup(self):
        from google.cloud import storage
        self.storage_client = storage.Client()

    def process(self,bc):


        """Lists all the blobs in the bucket."""
        #from google.cloud import storage
        #self.storage_client = storage.Client()
        blobs = self.storage_client.list_blobs('pre-ptpdocmgmt-bucket', prefix = 'poc-inputfiles')
        json_paths = []
        for blob in blobs:
          #json_paths.append(f"gs://{bucket_name}/{blob.name}")
            json_paths.append(f"{blob.name}")
        yield json_paths


# In[41]:


# class ReadFileContent(beam.DoFn):
#
#     def start_bundle(self):
#         from google.cloud import storage
#
#     #def setup(self):
#
#         self.storage_client = storage.Client()
#
#     def process(self, file_name):
#         bucket = self.storage_client.get_bucket('paramountpoc-exportjson')
#
#         for i in file_name[1:]:
#
#             blob = bucket.get_blob(i)
#             h = json.loads(blob.download_as_string())
#             h.update(file_loc = i)
#             yield h

            #yield json.loads(blob.download_as_string())

class docai(beam.DoFn):

    def start_bundle(self):
        from google.cloud import storage
        self.storage_client = storage.Client()
        from google.cloud import documentai
        from google.api_core.client_options import ClientOptions
        from google.api_core.exceptions import RetryError
        from google.cloud import documentai, storage
        import re

    def process(self,f_name):

        from google.cloud import storage
        self.storage_client = storage.Client()
        from google.cloud import documentai
        from google.api_core.client_options import ClientOptions
        from google.api_core.exceptions import RetryError
        from google.cloud import documentai, storage
        import re

        project_id = 'pre-ptpdocmgmt-1059'
        location = 'us'
        processor_id = '73efe0a727c607ea'

        gcs_output_bucket = 'gs://paramountpoc-exportjson'
        gcs_output_uri_prefix= 'poc-testjsons'
        #file_path = 'gs://pre-ptpdocmgmt-bucket/Single-test-new/Invoice_23273615_20220621.PDF'
        #file_path = 'gs://pre-ptpdocmgmt-bucket/try_test1'
        #file_path = 'gs://temp/try_test
        input_mime_type = 'application/pdf'
        processor_version_id = '4ccaf04430ed51e5'
        field_mask = None
        timeout: int = 400
        opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")
        client = documentai.DocumentProcessorServiceClient(client_options=opts)


        for i in f_name[1:]:
            gcs_input_uri= 'gs://pre-ptpdocmgmt-bucket/'+ i
            print(gcs_input_uri)


        # You must set the api_endpoint if you use a location other than 'us', e.g.:
        #opts = ClientOptions(api_endpoint=f"{location}-documentai.googleapis.com")

        #client = documentai.DocumentProcessorServiceClient(client_options=opts)

            gcs_document = documentai.GcsDocument(
              gcs_uri=gcs_input_uri, mime_type=input_mime_type
            )

        #Load GCS Input URI into a List of document files
            gcs_documents = documentai.GcsDocuments(documents=[gcs_document])
            input_config = documentai.BatchDocumentsInputConfig(gcs_documents=gcs_documents)

        # NOTE: Alternatively, specify a GCS URI Prefix to process an entire directory
        #
        # gcs_input_uri = "gs://bucket/directory/"
        # gcs_prefix = documentai.GcsPrefix(gcs_uri_prefix=gcs_input_uri)
        # input_config = documentai.BatchDocumentsInputConfig(gcs_prefix=gcs_prefix)
        # #

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

            #storage_client = self.storage.Client()

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
                output_blobs = self.storage_client.list_blobs(output_bucket, prefix=output_prefix)

#                 bucket = self.storage_client.get_bucket('paramountpoc-exportjson')

#                 blob = bucket.get_blob(i)

#                 h = json.loads(blob.download_as_string())
#                 h.update(file_loc = i)
                for blob in output_blobs:
                    h = json.loads(blob.download_as_string())
                    h.update(file_loc = blob.name)
                    source_bucket = self.storage_client.get_bucket("pre-ptpdocmgmt-bucket")
                    blob = source_bucket.get_blob(i)
                    destination_bucket = self.storage_client.get_bucket("paramountpoc-processedpdf")
                    #blob = bucket.blob("PO ,Vendor,Cost center and  WBS.csv")
                    #data = blob.download_as_bytes()
                    blob_copy = source_bucket.copy_blob(blob, destination_bucket, blob.name)
                    source_bucket.delete_blob(i)

            #print(type(h))
                    yield h

# In[42]:


class jsontoxml2(beam.DoFn):

    def start_bundle(self):
        from google.cloud import storage
        import xml.etree.ElementTree as ET
    #def setup(self):
        self.storage_client = storage.Client()
        from datetime import datetime as dt
        from datetime import datetime
        import pytz

    def process(self, data):

        import xml.etree.ElementTree as ET
    #def setup(self):
        #self.storage_client = storage.Client()
        from datetime import datetime as dt
        from datetime import datetime
        import pytz

        print('****')
        print(data['invoice_id'])
    #print(data)

        bucket_name = 'paramount_poc_xml_files'
        bucket = self.storage_client.get_bucket('paramount_poc_xml_files')
        blob = bucket.get_blob('sample.xml')
        xml_file = blob.download_as_string()
        aware_us_central = datetime.now(pytz.timezone('US/Central'))
        iso_date = aware_us_central.replace(microsecond=0).isoformat()
        date = str(datetime.now().date()).replace('-',"")
        #fname = date + str(data['invoice_id']) + ".PDF"
        fname = str((data['file_loc'].replace('.json','').split('/'))[-1]) + ".PDF" + date + str(datetime.now().time()).replace(':',"").split(".")[0]

        tree = ET.ElementTree(ET.fromstring(xml_file))
        root = tree.getroot()
        root.set('timestamp',iso_date)

        root.set("payloadID", fname )


    #sender details
        for temp in root.iter('From'):
            for fromid in temp.iter('Identity'):
                fromid.text = data['vendorID']              # private id of sender

        for Header in root.iter('Correspondent'):
            contact = Header.find("Contact")
            contact.set('addressID',"")                 # address id of the billed from company
            for name in Header.iter('Name'):
                name.text = data['supplier_name']                            # Name of the billed from company

      #invoice details
        for Invdet in root.iter('InvoiceDetailRequestHeader'):
            Invdet.set('invoiceID',data['invoice_id'])
            Invdet.set('invoiceDate',data['invoice_date'] + "T00:00:00-06:00")

    #billed to details

        for invoicepartner in root.iter('InvoiceDetailRequestHeader'):
            s = invoicepartner.findall('InvoicePartner')

      #billed to details
            billto = s[0]
            contact = billto.find("Contact")
            contact.set('addressID',data['companyCode'])                 # address id of the billed to company
            for name in billto.iter('Name'):
                name.text = data['receiver_name']                            # Name of the billed to company
            address = contact.find('PostalAddress')
            street = address.find('Street')
            street.text = data['receiver_street']                            # street of billed to company
            city = address.find('City')
            city.text = data['receiver_city']                              # city of billed to company
            state = address.find('State')
            state.text = data['reciever_state']                             # state of billed to company
            pcode = address.find('PostalCode')
            pcode.text = data['receiver_pincode']                             # postal code of billed to company
            country = address.find('Country')
            country.text = data['receiver_country']                           # country of billed to company
            country.set('isoCountryCode', "US" )            # data['receiver_country'] country code of billed to company

      #remitted to details
            remitto = s[1]
            contact = remitto.find("Contact")
            contact.set('addressID',str(data['vendorID']+":"+data['vendorID']))                 # address id of the remitted to company
            for name in remitto.iter('Name'):
                name.text = data['supplier_name']                            # Name of the remitted to company
            address = contact.find('PostalAddress')
            street = address.find('Street')
            street.text = data['supplier_street']                            # street of remitted to company
            city = address.find('City')
            city.text = data['supplier_city']                              #city of remitted to company
            state = address.find('State')
            state.text = data['supplier_state']                             # state of remitted to company
            pcode = address.find('PostalCode')
            pcode.text = data['supplier_postalcode']                             # postal code of remitted to company
            country = address.find('Country')
            country.text = data['supplier_country']                           # country of remitted to company
            country.set('isoCountryCode',"US")            #data['supplier_country'] country code of remitted to company

      #sold to details

            soldto = s[2]
            contact = soldto.find("Contact")
            contact.set('addressID',"")                 # address id of the sold to company
            for name in soldto.iter('Name'):
                name.text = ""                            # Name of the sold to company
            address = contact.find('PostalAddress')
            street = address.find('Street')
            street.text = ""                            # street of sold to company
            city = address.find('City')
            city.text = ""                              # city of sold to company
            state = address.find('State')
            state.text = ""                             # state of sold to company
            pcode = address.find('PostalCode')
            pcode.text = ""                             # postal code of sold to company
            country = address.find('Country')
            country.text = ""                           # country of sold to company
            country.set('isoCountryCode',"")            # country code of sold to company
            Email = contact.find("Email")
            Email.text = data['supplier_email']

        for filename in root.iter('URL'):
            filename.text = "cid:" + str((data['file_loc'].replace('.json','').split('/'))[-1]) + ".PDF@Attachment"                  # Name of the file  ####

    #Order info and amount details
        for Orddet in root.iter('OrderIDInfo'):
            Orddet.set('orderDate','')  #purchase order date
            Orddet.set('orderID',data['purchase_order'])  #purchase order id

        for temp in root.iter('InvoiceDetailItem'):
            temp.set('invoiceLineNumber',data['line_item/unit'][0])           #item number
            temp.set('quantity',"")
            if(len(data['line_item/quantity'])>=1):
                temp.set('quantity',data['line_item/quantity'][0])
            #print(temp.attrib)
            temp2 = temp.find('UnitPrice')
            #print({x.tag for x in itdet.findall(temp.tag+"/*")})
            Amtdet = temp2.find('Money')
            Amtdet.text = data['line_item/amount'][0]                   #temp_f['line_item/amount'] #unit price of item
            Amtdet.set('currency',data['currency'])
            temp3 = temp.find('NetAmount')
            IdIRtag = temp.find('InvoiceDetailItemReference')
            #IdIRtag.set('lineNumber',data['line_item/unit'][0] )
            # ET.SubElement(IdIRtag,'ItemID')
            item = IdIRtag.find('ItemID')
            # ET.SubElement(item,'SupplierPartID')
            # ET.SubElement(IdIRtag,'Description', lang="en-US")
            itemDesc = IdIRtag.find('Description')
            itemDesc.text = ""
            if(len(data['line_item/description'])==1):
                itemDesc.text = data['line_item/description'][0]
        #print({x.tag for x in itdet.findall(temp.tag+"/*")})
            netAmtdet = temp3.find('Money')
            netAmtdet.text = data['line_item/amount'][0]                   #temp_f['line_item/amount'] #unit price of item
            netAmtdet.set('currency',data['currency'])


        for itdet in root.iter('InvoiceDetailOrder'):

            print('*')
        # idoitag = ET.SubElement(temp,'InvoiceDetailOrderInfo')
        # oiitag = ET.SubElement(temp,'OrderIDInfo',orderDate="", orderID="")

            for j in range(1,len(data['line_item/unit'])): #len(data['line_item/unit'])
                #print("len(data['line_item/unit']",j)
                if(len(data['line_item/quantity'])>1):
                    iditag = ET.SubElement(itdet,'InvoiceDetailItem', invoiceLineNumber=data['line_item/unit'][j], quantity=data['line_item/quantity'][j])
                else:
                    iditag = ET.SubElement(itdet,'InvoiceDetailItem', invoiceLineNumber=data['line_item/unit'][j], quantity="")

                UnitOfMeasure = ET.SubElement(iditag,'UnitOfMeasure')
                UnitOfMeasure.text = ""
                iditag1 = itdet.findall('InvoiceDetailItem')
                iditag = iditag1[len(iditag1)-1]
                ET.SubElement(iditag,'UnitPrice')
                UnitPrice = iditag.find('UnitPrice')
                ET.SubElement(UnitPrice,'Money', currency=data['currency'])
                UnitPriceAmount = UnitPrice.find('Money')
                UnitPriceAmount.text = data['line_item/amount'][j]
                ET.SubElement(iditag,'InvoiceDetailItemReference', lineNumber=data['line_item/unit'][j])
                IdIRtag = iditag.find('InvoiceDetailItemReference')
                ET.SubElement(IdIRtag,'ItemID')
                item = IdIRtag.find('ItemID')
                ET.SubElement(item,'SupplierPartID')
                ET.SubElement(IdIRtag,'Description', lang="en-US")
                itemDesc = IdIRtag.find('Description')
                itemDesc.text = ""
                if(len(data['line_item/description'])>1):
                    itemDesc.text = data['line_item/description'][j]
                ET.SubElement(iditag,'NetAmount')
                itemAmt = iditag.find('NetAmount')
                ET.SubElement(itemAmt,'Money', currency=data['currency'])
                itemAmount = itemAmt.find('Money')
                itemAmount.text = data['line_item/amount'][j]





        for temp in root.iter('InvoiceDetailSummary'):
            temp2 = temp.find('SubtotalAmount')
            invf = temp2.find('Money')
            invf.text = data['total_amount']   #temp_f['total_amount']      # Subtotal amount
            invf.set('currency',data['currency']) #temp_f['currency']     #currency code
            temp2 = temp.find('Tax')
            invf = temp2.find('Money')
            invf.text = ""  #temp_f['total_amount']      # tax amount
            invf.set('currency',data['currency']) #temp_f['currency']     #currency code
            temp2 = temp.find('GrossAmount')
            invf = temp2.find('Money')
            invf.text = data['total_amount']            #temp_f['total_amount'] #gross amount
            invf.set('currency',data['currency']) #temp_f['currency']     #currency code
            temp2 = temp.find('NetAmount')
            invf = temp2.find('Money')
            invf.text = data['total_amount']             #temp_f['total_amount'] #net amount
            invf.set('currency',data['currency']) #temp_f['currency']     #currency code

        elem = root
        level = 0

        def indent(elem, level=0):
            i = "\n" + level*"  "
            if len(elem):
                if not elem.text or not elem.text.strip():
                    elem.text = i + "  "
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
                for elem in elem:
                    indent(elem, level+1)
                if not elem.tail or not elem.tail.strip():
                    elem.tail = i
            else:
                if level and (not elem.tail or not elem.tail.strip()):
                    elem.tail = i

        indent(root)

        blob = bucket.blob((data['file_loc'].replace('.json','').split('/'))[-1]+".xml")
        blob.upload_from_string(ET.tostring(root, encoding='UTF-8',xml_declaration=True, method='xml').decode('UTF-8'),content_type='application/xml')

        source_bucket = self.storage_client.get_bucket("paramountpoc-exportjson")
        blob = source_bucket.get_blob(data['file_loc'])
        destination_bucket = self.storage_client.get_bucket("paramountpoc-processedjson")
        #blob = bucket.blob("PO ,Vendor,Cost center and  WBS.csv")
        #data = blob.download_as_bytes()
        blob_copy = source_bucket.copy_blob(blob, destination_bucket, blob.name)
        source_bucket.delete_blob(data['file_loc'])

# In[43]:


class jsontodic(beam.DoFn):

    def start_bundle(self):
        from google.cloud import storage
        import pandas
        self.storage_client = storage.Client()
        import io
        from datetime import datetime
        import pytz
        from datetime import datetime as dt
        #from apache_beam.dataframe.io import read_csv

    def process(self,d):

        import pandas
        import io
        from datetime import datetime
        import pytz
        from datetime import datetime as dt

        bucket = self.storage_client.bucket("paramount_poc_sap_data")
        blob = bucket.blob("PO ,Vendor,Cost center and  WBS.csv")
        data = blob.download_as_bytes()
        df = pandas.read_csv(io.BytesIO(data))


        vendorID_dic = dict(zip(df['Purchasing Document'], df.Vendor))
        #companyCode_df = gcp_csv_to_df(self.storage_client,"paramount_poc_sap_data","PO ,Vendor,Cost center and  WBS.csv")
        companyCode_dic = dict(zip(df['Purchasing Document'], df['Compnay code']))
        keys_values = vendorID_dic.items()
        vendorID_dic = {str(key): str(value) for key, value in keys_values}
        keys_values = companyCode_dic.items()
        companyCode_dic = {str(key): str(value) for key, value in keys_values}




        temp_f={'file_loc': "",'companyCode':"" , 'supplier_name':"", 'supplier_street':"", 'invoice_id':"", 'supplier_postalcode':"","vendorID":"", 'supplier_country':"", 'supplier_city':"", 'supplier_state':"", 'invoice_date':"", 'supplier_email':"", 'receiver_name':"", 'receiver_street':"", 'receiver_city':"", 'reciever_state':"", 'receiver_pincode':"", 'receiver_country':"", 'due_date':"", 'purchase_order':"", 'currency':"", 'line_item/unit':[], 'line_item/amount':[], 'line_item/description':[],'line_item/quantity':[],'line_item/unit_price':[], 'total_amount':""}

        temp_f['file_loc'] = d['file_loc']

        for i in d['entities']:
            if(i['type'] == 'invoice_date'):
                if('mentionText' in i):
                    dateobject = datetime.strptime(i['mentionText'], '%b/%d/%Y').date()
                    d = dateobject.strftime('%Y-%m-%d')
                    temp_f[i['type']] = d

            elif(i['type'] == 'line_item'):
                 for j in i['properties']:
                    if 'mentionText' in j:
                        temp_f[j['type']].append(j['mentionText'])

            else:
                if('mentionText' in i):
                    temp_f[i['type']] = i['mentionText']


        if(temp_f['purchase_order'] in vendorID_dic):
            print("hit on vendorid for" + temp_f["invoice_id"])
            temp_f['vendorID'] = vendorID_dic[temp_f['purchase_order']]
        if(temp_f['purchase_order'] in companyCode_dic):
            print("hit on companycode for" + temp_f["invoice_id"])
            temp_f['companyCode'] = companyCode_dic[temp_f['purchase_order']]

        yield temp_f


# In[44]:


#def run():
# Set up Apache Beam pipeline options.
options = PipelineOptions()

# environment.
_, options.view_as(GoogleCloudOptions).project = google.auth.default()

# Set the Google Cloud region to run Dataflow.
options.view_as(GoogleCloudOptions).region = 'us-central1'

# Choose a Cloud Storage location.

dataflow_gcs_location = 'gs://dataflow-job-paramount'
options.view_as(GoogleCloudOptions).staging_location = '%s/staging' % dataflow_gcs_location
options.view_as(GoogleCloudOptions).temp_location = '%s/temp' % dataflow_gcs_location


p1 = beam.Pipeline()

results = (

    p1 | 'Create empty list' >> beam.Create([''])
      | 'Create input list of json paths' >> beam.ParDo(list_blobs2())
      #| 'Create input list of json paths' >> beam.Create([''])

      | 'parse each pdf file and create json' >> beam.ParDo(docai())
      | 'extract required data from json and sap files and load it into a dictionary' >> beam.ParDo(jsontodic())
      | "load the data to xml and store it in GCP" >> beam.ParDo(jsontoxml2())
  )

runner = DataflowRunner()
runner.run_pipeline(p1, options=options)

# In[45]:


# p.run()
# #p.run()

# if __name__ == '__main__':
#     #logging.getLogger().setLevel(logging.INFO)
#     run()


# In[8]:



# def gcp_csv_to_df(bucket_name, source_file_name):
#     storage_client = storage.Client()
#     bucket = storage_client.bucket(bucket_name)
#     blob = bucket.blob(source_file_name)
#     data = blob.download_as_bytes()
#     df = pd.read_csv(io.BytesIO(data))
#     print(f'Pulled down file from bucket {bucket_name}, file name: {source_file_name}')
#     return df


# In[ ]:





# In[9]:


# df2 = gcp_csv_to_df("paramount_poc_sap_data","PO ,Vendor,Cost center and  WBS.csv")


# In[ ]:





# In[13]:


# df2.columns


# In[15]:


# vendorID_df = gcp_csv_to_df("paramount_poc_sap_data","PO ,Vendor,Cost center and  WBS.csv")
# vendorID_dic = dict(zip(vendorID_df['Purchasing Document'], vendorID_df.Vendor))
# companyCode_df = gcp_csv_to_df("paramount_poc_sap_data","PO ,Vendor,Cost center and  WBS.csv")
# companyCode_dic = dict(zip(companyCode_df['Purchasing Document'], companyCode_df['Compnay code']))
# keys_values = vendorID_dic.items()
# vendorID_dic = {str(key): str(value) for key, value in keys_values}
# keys_values = companyCode_dic.items()
# companyCode_dic = {str(key): str(value) for key, value in keys_values}


# In[28]:


# companyCode_dic


# In[ ]:
