# Token Credential Sample

This sample demonstrates how to configure a Identity based connection when using DurableTask.AzureStorage v1.x as the orchestration provider for a Durable Task project.

> Note:
> Identity based connection **is not supported** with .NET framework 4.x with DurableTask.AzureStorage v1.x

## Configuration Prerequisites

Before running this sample, you must

1. Create a new Azure Storage account or reuse an existing one
2. Create your identity in the Azure Portal. Detailed instructions can be found [here](https://learn.microsoft.com/en-us/entra/identity-platform/quickstart-register-app?tabs=certificate)
3. Assign Role-based Access Controls (RBAC) of the storage account created in step 1 to the identity created in step 2 with [these instructions](https://learn.microsoft.com/azure/role-based-access-control/role-assignments-portal-managed-identity#Overview).  
        * Storage Queue Data Contributor
        * Storage Blob Data Contributor
        * Storage Table Data Contributor
4. Add the identity required information to your app's configuration.
5. Set `AccountName` to the name of the storage account. AccountName can be replaced with Storage Account BlobServiceUri, TableServiceUri and QueueServiceUri. 
