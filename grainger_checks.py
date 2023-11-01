# %%


import pandas as pd
import numpy as np
import datetime
import tempfile
import os
from io import StringIO
from azure.storage.blob import BlobServiceClient, BlobClient, BlobLeaseClient


connectionString1 = "DefaultEndpointsProtocol=https;AccountName=nfinsdata;AccountKey=fkIjwa6uiAZBanIeeK8MtqdrGg/04sXKgCGCCHDX0yM+D6h/slJO2cRs5l1kb/k+14wyJtG2elvs+AStl1f7vQ==;EndpointSuffix=core.windows.net"
blobConnect = BlobServiceClient.from_connection_string(connectionString1)
expShipContainer = "exp-ship-date"
expShipConnect = blobConnect.get_container_client(expShipContainer)
graingerCheckContainer = "grainger-checks"

expOpBlob = "exp_ship_date_op.csv"

exp_dataBlob_client = expShipConnect.get_blob_client(expOpBlob)

exp_dataBlob_data = exp_dataBlob_client.download_blob().content_as_text()

exp_df = pd.read_csv(StringIO(exp_dataBlob_data))


# %%


# %%
today = datetime.datetime.today()
exp_df['Ship Date'] = pd.to_datetime(exp_df['Ship Date'])
exp_df['Expected Ship Date'] = pd.to_datetime(exp_df['Expected Ship Date'])

# %%
exp_df = exp_df[(exp_df['Name'].isin(['Grainger - Branch Office', 'Grainger - Sourcing'])) & (exp_df['Allocation Status'].isin(['Order More', 'Parent on Purchase order', 'Available On Purchase Order'])) & (exp_df['Ship Date'] > today) & ((exp_df['Expected Ship Date'] > exp_df['Ship Date']) | exp_df['Expected Ship Date'].isna())]

# %%
with tempfile.NamedTemporaryFile(delete=False) as temp_file:
  exp_df.to_csv(temp_file.name,index=False)
  outputPath = temp_file.name

uploadBlobClient = blobConnect.get_blob_client(container=graingerCheckContainer, blob = "graingerChecks.csv")
with open(file=outputPath,mode="rb") as outputData:
    uploadBlobClient.upload_blob(outputData, overwrite=True)


