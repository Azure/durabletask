Breaking changes: VNext branch now merged to master!!
======================


The vnext branch has now been merged into master with the follwing PR:
https://github.com/Azure/durabletask/pull/104

This merge contains some major refactoring primarily around building a provider model that decouples the core framework from Service Bus. As a result there are breaking changes both in terms of APIs as well as wire format. 

To ugprade you will have to drop and recreate durable task artifacts. More details in an upcoming wiki entry soon. 

Old master code will always be available in the rel/v1 branch. 

Note that we will be creating and publishing a new nuget package with the current master and will be bumping up the major version number to 2.x.x.x. 

Durable Task Framework
======================

[![Join the chat at https://gitter.im/azure/durabletask](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/azure/durabletask?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) 

This framework allows users to write long running persistent workflows in C# using the async/await capabilities.

It is used heavily within various teams at Microsoft to reliably orchestrate long running provisioning, monitoring and management operations. The orchestrations scale out linearly by simply adding more worker machines. 

By open sourcing this project we hope to give the community a very cost-effective alternative to heavy duty workflow systems. We also hope to build an ecosystem of providers and activities around this simple yet incredibly powerful framework.

This project has adopted the [Microsoft Open Source Code of Conduct](https://opensource.microsoft.com/codeofconduct/). 
For more information see the [Code of Conduct FAQ](https://opensource.microsoft.com/codeofconduct/faq/) or contact [opencode@microsoft.com](mailto:opencode@microsoft.com) with any additional questions or comments.

The framework binaries are available as a NuGet package at:
https://www.nuget.org/packages/DurableTask

<b>Development Notes</b>

To run unit tests, you must specify your Service Bus connection string for the tests to use. You can do this via the **ServiceBusConnectionString** app.config value in the test project, or by defining a **DurableTaskTestServiceBusConnectionString** environment variable. The benefit of the environment variable is that no temporary source changes are required. 

The associated wiki contains more details about the framework:
https://github.com/Azure/durabletask/wiki

<b>TODO</b>

We could have spent a lot of time cleaning up the code and teasing apart various layers but we opted for shipping this earlier and cleaning up post-commit. Consequently there are a lot of TODOs :)

The community is welcome to take a stab and send pull requests on these items (and more):

* Provider model for the TaskOrchestrationDispatcher. ServiceBus should just be one of the providers. We want providers on top of Azure Storage, Amazon's Simple Workflow Service, MS-SQL.
* Provider model for the instance store. Currently it is tied to Azure tables. Need providers for MS-SQL and probably some NoSQL stores.
* Out-of-box integration with standard IoC libraries (AutoFac, Unity etc) for registering TaskOrchestration and TaskActivity objects.
* Better indexing support for the Azure table based instance store
* Change tracing to use EventSource rather than TraceSource
* Library of standard task activities and sub orchestrations that perform domain specific operations e.g. a SaaS connector library for integrating with Twilio, FB, Twitter, Skype etc OR a library for managing cloud resources like AWS EC2 instances or S3 buckets etc.
* Consistent serialization across the board, standardize on json.net. We have a mixture of DCS & Json.Net right now.
* Separate out test fx related code (e.g. mock test host) into a separate project and consequently a different nuget package.
* Add replay capability which enables users to replay an orchestration giving an execution history
* Better organization of tests, right now a bunch of tests are placed under the wrong test classes
* ...many more :)
  
<b>Forum</b>

Please post feedback/comments at gitter.

[![Join the chat at https://gitter.im/azure/durabletask](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/azure/durabletask?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge) 
