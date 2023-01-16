### Contains a class that represents the abstraction of interactions with the Microsoft Genomics service
### strongly inspired by:
### https://github.com/colbyford/msgen/blob/master/R/workflow.R

import requests
from datetime import datetime, timedelta
from azure.storage.blob import generate_blob_sas
from azure.mgmt.storage import StorageManagementClient

class azureGenomics:
    """
    Abstraction of interactions with Microsoft Genomics service
    """
    def __init__(self, 
                 msgen_subkey, 
                 region, 
                 inp_sa_name, 
                 inp_sa_key, 
                 inp_cont_name, 
                 out_sa_name = None, 
                 out_sa_key = None, 
                 out_cont_name = None):
        """
        Require subscription key and region on initiation
        """
        self.subkey, self.region = msgen_subkey, region
        self.inp_sa_name, self.inp_sa_key, self.inp_cont_name = inp_sa_name, inp_sa_key, inp_cont_name
        
        if not out_sa_name:
            self.out_sa_name, self.out_sa_key, self.out_cont_name = inp_sa_name, inp_sa_key, inp_cont_name
        else:
            self.out_sa_name, self.out_sa_key, self.out_cont_name = out_sa_name, out_sa_key, out_cont_name

        self._get_blob_sas()

    def submit_workflow(self, 
                        blobname_R1,
                        blobname_R2,
                        process = "gatk", 
                        reference = "hg38m1"):
        """
        Submit a workflow and point output to a storage account
        """
        reference = "R=" + reference
        requestbody = self._get_sample_request(process,
                                               reference,
                                               blobname_R1,
                                               blobname_R2)

        url = "https://" + self.region + ".microsoftgenomics.net/api/workflows/"
        headers = {"Ocp-Apim-Subscription-Key": self.subkey,
                   "Content-Type": "application/json",
                   "User-Agent": "Microsoft Genomics python3 Client"}
        
        result = requests.post(url = url,
                               headers = headers,
                               data = requestbody)

        print(f"Request sent with code {result.status_code}")

    def _get_sample_request(process,
                            reference,
                            blobname_R1,
                            blobname_R2):
        """
        Assemble the body of the actual request
        """
        blobname_R1_full = blobname_R1 + self.inp_sas
        blobname_R2_full = blobname_R2 + self.inp_sas

        return {
            "WorkflowClass": "",
            "Description": "Submitted by python3 client",
            "InputArgs": {
                "BLOBNAMES_WITH_SAS": ",".join([blobname_R1_full, blobname_R2_full]),
                "ACCOUNT": self.inp_sa_name,
                "CONTAINER": self.inp_cont_name,
                "BLOBNAMES": ",".join([blobname_R1_full, blobname_R2_full])
            },
            "InputStorageType": "AZURE_BLOCK_BLOB",
            "OutputArgs": {
                "ACCOUNT": self.out_sa_name,
                "CONTAINER": self.out_cont_name,
                "CONTAINER_SAS": self.out_sas,
                "OUTPUT_INCLUDE_LOGFILES": True,
                "OVERWRITE": False,
                "OUTPUT_FILENAME_BASE": ""
            },
            "OutputStorageType": "AZURE_BLOCK_BLOB",
            "ProcessArgs": reference,
            "Process": process,
            "OptionalArgs": dict(),
            "IgnoreAzureRegion": None
        }

    def _get_blob_sas(self):
        """
        Get SAS keys for input and output blobs
        """
        time_start = datetime.now().strftime("%Y-%m-%d")
        time_expire = (datetime.now() + timedelta(days = 1)).strftime("%Y-%m-%d")

        inp_sas = generate_blob_sas(account_name = self.inp_sa_name,
                                    account_key = self.inp_sa_key,
                                    start = time_start,
                                    expiry = time_expire,
                                    permission = "rwcdl")

        out_sas = generate_blob_sas(account_name = self.out_sa_name,
                                    account_key = self.out_sa_key,
                                    start = time_start,
                                    expiry = time_expire,
                                    permission = "rwcdl")

        self.inp_sas, self.out_sas = inp_sas, out_sas