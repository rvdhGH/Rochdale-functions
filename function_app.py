import azure.functions as func
import logging
import pandas as pd
import openpyxl
from azure.storage.blob import BlobServiceClient, BlobClient
from io import BytesIO
import time

app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)

def csv_to_xlsx(connection_string, container_name, blob_name):
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    blob_client = blob_service_client.get_blob_client(container_name, blob_name)
    download_stream = blob_client.download_blob().readall()
    df = pd.read_csv(BytesIO(download_stream))
    output = BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer,index=False)
    xlsx_blob_name = blob_name.rsplit('.', 1)[0] + '.xlsx'
    xlsx_blob_client = blob_service_client.get_blob_client(container_name, xlsx_blob_name)
    output.seek(0)
    xlsx_blob_client.upload_blob(output, overwrite=True)
    
@app.route(route="CSVtoExcel", auth_level=func.AuthLevel.ANONYMOUS) 
def CSVtoExcel(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    body = req.get_json()
    connection_string = 'BlobEndpoint=https://pcsvtoxlsx.blob.core.windows.net/;QueueEndpoint=https://pcsvtoxlsx.queue.core.windows.net/;FileEndpoint=https://pcsvtoxlsx.file.core.windows.net/;TableEndpoint=https://pcsvtoxlsx.table.core.windows.net/;SharedAccessSignature=sv=2024-11-04&ss=bfqt&srt=c&sp=rwdlacupiytfx&se=2027-06-16T23:36:42Z&st=2025-06-16T15:36:42Z&spr=https&sig=lMJZuhA4yAYum1u8uVvl9g2kA63a0DwdmrhOU8wIKlw%3D'
    container_name = 'kwh'
    blob_name = body['blob_folder_and_name']
 
    if connection_string and container_name and blob_name:
        csv_to_xlsx(connection_string, container_name, blob_name)
        return func.HttpResponse(f"CSV file {blob_name} converted to XLSX successfully.")
    else:
        return func.HttpResponse(
             "Please pass a blob_folder_and_name in the query string or in the request body.",
             status_code=400
        )
