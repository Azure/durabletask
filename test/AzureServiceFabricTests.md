# Running Azure Service Fabric Tests Locally

## Prerequisites

### 1. Install Service Fabric SDK

Follow the official guide to install the Service Fabric SDK and runtime:

https://learn.microsoft.com/en-us/azure/service-fabric/service-fabric-get-started

### 2. Start the Local Cluster (5-Node Configuration)

1. Look for the **Service Fabric Local Cluster Manager** tray icon in the Windows system tray.
2. Right-click the tray icon.
3. Select **Start Local Cluster**.
4. Choose the **5 Node** configuration.
5. Wait for the cluster to finish initializing — the tray icon will change to indicate it's running.

### 3. Build the Solution

```powershell
dotnet build DurableTask.sln -c Debug -p:Platform=x64
```

### 4. Package the Test Application

1. Open the solution in Visual Studio.
2. In Solution Explorer, right-click **TestApplication** (`test\TestFabricApplication\TestApplication\TestApplication.sfproj`).
3. Click **Package**.

This produces the Service Fabric application package needed by the integration tests.

### 5. Run the Tests

```powershell
# Unit tests
dotnet test test\DurableTask.AzureServiceFabric.Tests\DurableTask.AzureServiceFabric.Tests.csproj -c Debug -p:Platform=x64

# Integration tests (requires the local cluster and packaged application)
dotnet test test\DurableTask.AzureServiceFabric.Integration.Tests\DurableTask.AzureServiceFabric.Integration.Tests.csproj -c Debug -p:Platform=x64
```

## Troubleshooting

### Binding redirect / assembly load errors

The `net48` test projects require binding redirects for assemblies with revision-number mismatches. If you see `FileNotFoundException` or `ReflectionTypeLoadException` errors, verify that `App.config` exists in each test project with the appropriate `<bindingRedirect>` entries.

The binding redirect files are located at:
- `test/DurableTask.AzureServiceFabric.Tests/App.config` (unit tests)
- `test/DurableTask.AzureServiceFabric.Integration.Tests/App.config` (integration tests)

### Cluster not reachable

If tests fail to connect to the local cluster, verify the cluster is running by opening **Service Fabric Explorer** at http://localhost:19080.
