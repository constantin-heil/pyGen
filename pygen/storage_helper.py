from azure.mgmt.storage import StorageManagementClient
from azure.identity import AzureCliCredential
import os
import random
from sys import exit

class provisionStorage:
    """
    Encapsulates provioning of storage accounts and generation of SAS keys
    """
    def __init__(self, rg_name, sa_name = None, region = "westeurope"):
        self.rg_name, self.sa_name, self.region = rg_name, sa_name, region

    def provision_storage(self):
        """
        Provision a new storage account under a resource group
        """
        credential = AzureCliCredential()
        subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

        self.storage_client = StorageManagementClient(credential, subscription_id)

        if not self.sa_name:
            self.sa_name = f"storageaccount{random.randint(0, 10000):04}"

        availability_res = self.storage_client.storage_accounts.check_name_availability(
            {"name": self.sa_name}
        )

        if not availability_res.name_available:
            print(f"ERROR: Name {self.sa_name} is already taken")
            exit(1)
        
        sa_kwargs = {
            "location": self.region,
            "kind": "StorageV2",
            "sku": {
                "name": "Standard_LRS"
            }
        }

        poller = self.storage_client.storage_accounts.begin_create(self.rg_name, 
                                                              self.sa_name,
                                                              sa_kwargs)
        sa_result = poller.result()

        self.status_dic = {
            "SA_name": sa_result.name,
            "SA_region": sa_result.location,
            "RG": self.rg_name
        }

        access_keys = self.storage_client.storage_accounts.list_keys(self.rg_name,
                                                                self.sa_name)
        self.access_keys = {
            "primary": access_keys.keys[0].value,
            "secondary": access_keys.keys[1].value
        }

    def provision_blob_container(self, cont_name = None):
        """
        Provision blob container
        """
        if not cont_name:
            cont_name = f"container{random.randint(0, 10000):04}"

        cont_result = self.storage_client.blob_containers.create(
            self.rg_name,
            self.sa_name,
            cont_name,
            {}
        )

        return cont_name

    
        

    
