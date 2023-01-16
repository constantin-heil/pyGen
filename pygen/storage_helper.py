from azure.mgmt.storage import StorageManagementClient
from azure.identity import AzureCliCredential
import os
import random
import json
from sys import exit

class provisionStorage:
    """
    Encapsulates provioning of storage accounts and generation of SAS keys
    """
    def __init__(self, rg_name, sa_name = None, region = "westeurope"):
        self.rg_name, self.sa_name, self.region = rg_name, sa_name, region

    def _init_storage_client(self):
        """
        Internal function to create the storage client instance
        """
        credential = AzureCliCredential()
        subscription_id = os.environ["AZURE_SUBSCRIPTION_ID"]

        self.storage_client = StorageManagementClient(credential, subscription_id)

    def provision_storage(self):
        """
        Provision a new storage account under a resource group
        """
        self._init_storage_client()

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
        self._init_storage_client()
        
        if not cont_name:
            cont_name = f"container{random.randint(0, 10000):04}"

        cont_result = self.storage_client.blob_containers.create(
            self.rg_name,
            self.sa_name,
            cont_name,
            {}
        )

        return cont_name

    def __repr__(self):
        return f"{type(self).__name__}(storage_account={self.sa_name})"

    def status_to_json(self, json_fn = "provisionStorage.json"):
        """
        export the status dictionary to json, to allow loading from it
        """
        try:
            status_str = json.dumps(self.status_dic)
        except NameError:
            print("status_dic not defined for {str(self)}")
            exit(1)

        with open(json_fn, "w") as fh:
            fh.write(status_str)

    @classmethod
    def from_json(cls, json_fn):
        """
        Construct object from json status_dic
        """
        with open(json_fn, "r") as fh:
            status_dic = json.load(fh)

        return cls(status_dic["RG"],
                   status_dic["SA_name"],
                   status_dic["SA_region"])

    
        

    
