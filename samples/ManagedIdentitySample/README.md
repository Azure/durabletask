# Managed Identity Sample

This directory contains samples demonstrating how to use Azure Managed Identity for authentication with Azure Storage in Durable Task applications.

## Overview

Managed Identity provides a more secure alternative to connection strings by eliminating the need to store credentials. These samples show how to configure identity-based connections for both v1.x and v2.x versions of the Azure Storage provider.

## Samples

| Sample | Description |
| ------ | ----------- |
| [DTFx.AzureStorage v1.x](DTFx.AzureStorage%20v1.x/) | Legacy WindowsAzure.Storage SDK with Managed Identity |
| [DTFx.AzureStorage v2.x](DTFx.AzureStorage%20v2.x/) | Modern Azure.Storage.* SDK with TokenCredential |

## Prerequisites

Before running these samples, you must:

1. **Create an Azure Storage account** or reuse an existing one

2. **Create your identity** in the Azure Portal. Detailed instructions can be found in the [Microsoft Entra documentation](https://learn.microsoft.com/entra/identity-platform/quickstart-register-app?tabs=certificate)

3. **Assign Role-based Access Controls (RBAC)** to the identity with [these instructions](https://learn.microsoft.com/azure/role-based-access-control/role-assignments-portal-managed-identity#Overview):
   - Storage Queue Data Contributor
   - Storage Blob Data Contributor
   - Storage Table Data Contributor

4. **Configure the identity** in your app's configuration

5. **Set the storage account name** in your configuration. The account name can be replaced with individual service URIs (BlobServiceUri, TableServiceUri, QueueServiceUri)

## Code Examples

### DTFx.AzureStorage v1.x

```csharp
var credential = new DefaultAzureCredential();
var settings = new AzureStorageOrchestrationServiceSettings
{
    StorageAccountClientProvider = new ManagedIdentityStorageAccountClientProvider(
        storageAccountName, 
        credential)
};
```

> [!NOTE]
> Identity-based connection is **not supported** with .NET Framework 4.x when using DurableTask.AzureStorage v1.x

### DTFx.AzureStorage v2.x

```csharp
var credential = new DefaultAzureCredential();
var settings = new AzureStorageOrchestrationServiceSettings
{
    StorageAccountClientProvider = new StorageAccountClientProvider(
        new Uri($"https://{storageAccountName}.blob.core.windows.net"),
        credential)
};
```

## Additional Resources

- [Azure Storage Provider Documentation](../../docs/providers/azure-storage.md)
- [Azure Managed Identity Overview](https://learn.microsoft.com/azure/active-directory/managed-identities-azure-resources/overview)
