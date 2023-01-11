# pyGen: Microsoft genomics complete workflow

Methods and classes that serve to submit workflows to the Microsoft Genomics Azure service, starting with fastq files on local machine.

## class azureGenomics

Submission of the workflow proper to the Azure service.

```
azureGenomics(msgen_subkey, 
              region, 
              inp_sa_name, 
              inp_sa_key, 
              inp_cont_name, 
              out_sa_name = None, 
              out_sa_key = None, 
              out_cont_name = None)
```

## method storageProvisionUpload

Provision a storage account and upload files. If storage account exists create container and upload files.
