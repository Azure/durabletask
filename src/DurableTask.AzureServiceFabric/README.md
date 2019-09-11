# Service Fabric Reliable Collections Provider For Durable Task Framework

This page outlines the current status of the custom provider for Durable Task Framework based on [Service Fabric Reliable Collections](https://docs.microsoft.com/en-us/azure/service-fabric/service-fabric-reliable-services-reliable-collections).

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/).
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

One fundamental way Service Fabric provider differs from other providers from a user perspective is that:
With all other providers, the provider is an external store relative to Task Hub worker i.e., Task hub worker runs in a cluster and provider is hosted in a different cluster elsewhere and runs on different nodes.
However, with the Service fabric provider, the provider runs on the same cluster as Task hub worker (and Task hub client as well for now). The scale-out of task hub worker nodes should be planned with a regular service fabric partitioning model.

## What has been done?

* Scheduled Tasks, Timers, Sub orchestrations work!
* Fabric based instance store has limited functionality for now!
  * No support for history events.
  * Only the latest state for a given Orchestration instance is stored (previous state changes are overwritten by latest). [For a period of 24 hour time window]
  * State can be queried only for Orchestrations still running / pending OR completed within an hour ago.
  * State of orchestration that's completed a day ago will be cleaned up.

## What is pending?

* ContinueAsNew needs to be implemented.
* Providing support for Azure Storage instance store with Service Fabric based provider. (So that we can persist the state more permanantly)
* External events should work but not tested.
* Support for history event tracking.
* Support for querying orchestration state across executions (only makes sense after we add support for ContinueWith)
* No automatic lock expiry for activities / orchestrations that are fetched from store but neither completed nor abandoned. (It's not clear if this is really needed with fabric provider given task hub worker and provider run on the same node.)
* No equivalent of dead letter support (any bad activity / session will keep forever in the system).

## Development Notes

There are pre-requisites for setting up this project for developing or making contributions.

* Current recommended Visual Studio is 2017.
* Service fabric SDK should be installed.
  * If this is not installed, when opening the solution, you get a dialog saying .sfproj is not supported (This is a test application project used for functional tests to run.)
* In visual studio, the default test architecture should be set to x64 for detecting tests properly.
* There are 2 manual steps that must be done before running tests:
  * A 5 node local cluster must be setup using Service Fabric Local Cluster Manager (installed by SDK).
  * After the solution is built, the "TestFabricApplication" must be packaged using the context menu. (The packaged application will be deployed to local cluster as part of test setup)

## .Net Targets

Currently this project supports .Net 4.6.1, which is framework version of 'netstandard2.0'. Some of the dependency packages do not have support for 'netstandard2.0', once those packages support 'netstandard2.0' this project will also support the same.
  
## Main Contributors

* Pradeep Kadubandi
* Shankar Reddy Sama
* Kameswara Rao Tanneru
