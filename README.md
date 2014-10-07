Durable Task Framework
======================

This framework allows users to write long running persistent workflows in C# using the async/await capabilities.

It is used heavily within various teams in Microsoft to reliably orchestrate long running provisioning, monitoring and management operations. The orchestrations scale out linearly by simply adding more worker machines. 

By contributing this project we hope to give the community a very cost-effective alternative to heavy duty workflow systems. We also hope to build an ecosystem of providers and activities around this simple yet incredibly powerful framework.

<b>Development Notes</b>

To run unit tests, edit this in TestHelpers.cs and plugin your Service Bus connection string:

```static string connectionString = "<TODO: PLUGIN YOUR CONNECTION STRING HERE>";```

The code in this repo is a version of what was used to build this Nuget package:
http://www.nuget.org/packages/ServiceBus.DurableTask/

A document that describes the framework is described here:
http://abhishekrlal.files.wordpress.com/2013/06/service-bus-durable-task-framework-developer-guide.docx

Some samples are published here:
http://code.msdn.microsoft.com/windowsazure/Service-Bus-Durable-Task-07330399

Note that the doc and samples refer to 'Microsoft.ServiceBus.DurableTask' which is the pre-OSS version of the package. We will soon be publishing packages to Nuget based on the OSS version (i.e. this code).

<b>TODO</b>

We could have spent a lot of time cleaning up the code and teasing apart various layers but we opted for shipping this earlier and cleaning up post-commit. Consequently there are a lot of TODOs and there will be breaking changes.

Community is welcome to take a stab and send pull requests on these items.

* Provider model for the TaskOrchestrationDispatcher. ServiceBus should just be one of the providers. We want providers on top of Azure Storage, Amazon's Simple Workflow Service, MS-SQL.
* Provider model for the instance store. Currently it is tied to Azure tables. Need providers for MS-SQL and probably some NoSQL stores.
* Out-of-box integration with standard IoC libraries (AutoFac, Unity etc) for registering TaskOrchestration and TaskActivity objects.
* Better indexing support for the Azure table based instance store
* Change tracing to use EventSource rather than TraceSource
* Library of standard task activities and sub orchestrations that perform domain specific operations e.g. a SaaS connector library for integrating with Twilio, FB, Twitter, Skype etc OR a library for managing cloud resources like AWS EC2 instances or S3 buckets etc.
  

