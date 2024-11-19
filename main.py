from fastapi import FastAPI, HTTPException
import os
import pandas as pd
import requests
from azure.storage.blob import BlobServiceClient
from io import BytesIO

# Azure connection string
connection_string = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

# Logic App HTTP endpoint
logic_app_url = "https://prod-01.northcentralus.logic.azure.com:443/workflows/28e87b9b3a6a48b4802a034a6c80e732/triggers/When_a_HTTP_request_is_received/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2FWhen_a_HTTP_request_is_received%2Frun&sv=1.0&sig=1rvjCDsKgFbxuID37vb8xkU3uVFA10zNV0YUZCIi7ow"

app = FastAPI()

def load_reference_excel(container_name: str, blob_name: str) -> pd.DataFrame:
    """
    Load an Excel file from Azure Blob Storage into a pandas DataFrame.
    """
    try:
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        stream_downloader = blob_client.download_blob()
        data = stream_downloader.readall()
        ref_df = pd.read_excel(BytesIO(data))
        return ref_df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading Excel file: {str(e)}")

def list_blob_files(container_name: str, folder_name: str):
    """
    List all files in a specified folder within a container.
    """
    try:
        container_client = blob_service_client.get_container_client(container_name)
        blobs = container_client.list_blobs(name_starts_with=folder_name + "/")
        file_list = [blob.name for blob in blobs if blob.name.endswith(".xlsx")]
        return file_list
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error listing blob files: {str(e)}")

@app.post("/match_sales_rep_files/")
def match_sales_rep_files():
    """
    Match Sales Rep files in blob storage with names in the reference Excel file, then POST to Logic App.
    """
    try:
        # Hardcoded values
        container_name = "riyatest"
        folder_name = "2024-November-Processed"
        reference_excel_blob = "External Rep Agency Distribution Emails1 (1).xlsx"  # Replace with your actual blob name
        
        # Load reference Excel
        ref_df = load_reference_excel(container_name, reference_excel_blob)
        
        # Ensure the reference file has required columns
        if ref_df.columns[0] != "Company /External Sales Rep":
            raise HTTPException(status_code=400, detail="First column of reference Excel must be 'SalesRep Names'.")
        
        if len(ref_df.columns) < 4:
            raise HTTPException(status_code=400, detail="Reference Excel must have at least 4 columns (emails expected in 2nd and 4th columns).")
        
        # List files in blob storage
        blob_files = list_blob_files(container_name, folder_name)
        
        matches = []
        for _, row in ref_df.iterrows():
            sales_rep_name = row[0]
            email_1 = row[1]
            email_2 = row[3]
            
            for blob_name in blob_files:
                base_name = os.path.basename(blob_name)
                if base_name.startswith(sales_rep_name):
                    matches.append({
                        "SalesRepName": sales_rep_name,
                        "SalesRepFileName": base_name,
                        "Email1": email_1,
                        "Email2": email_2
                    })

        if matches:
            # Post each match to Logic App
            for match in matches:
                response = requests.post(logic_app_url, json=match)
                
                # Check if response is successful (202)
                if response.status_code != 202:
                    raise HTTPException(status_code=response.status_code, detail=response.text)
        
        return {"matches": matches}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "Sales Rep Matching API is running. Use POST /match_sales_rep_files/ to match files."}
