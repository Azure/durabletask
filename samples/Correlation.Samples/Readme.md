# Distributed Tracing for Durable Task

Distributed Tracing for Durable Task is a feature for enabling correlation propagation among orchestrations and activities. 
The key features of Distributed Tracing for Durable Task are:

- **End to End Tracing with Application Insights**: Support Complex orchestration scenario. Multi-Layered Sub Orchestration, Fan-out Fan-in, retry, Timer,  and more. 
- **Support Protocol**: [W3C TraceContext](https://w3c.github.io/trace-context/) and [Http Correlation Protocol](https://github.com/dotnet/corefx/blob/master/src/System.Diagnostics.DiagnosticSource/src/HttpCorrelationProtocol.md) 
- **Suppress Distributed Tracing**: No breaking change for the current implementation

Currently, we support [DurableTask.AzureStorage](https://w3c.github.io/trace-context/). 

![Overview](docs/images/overview.png)

# Getting Started

If you want to try Distributed Tracing with DurableTask.AzureStorage, you can find a document with a Handful of examples. 

 - [Intro](docs/getting-started.md)

# Developing Provider

If you want to implement Distributed Tracing for other DurableTask providers, Read [Develop Distributed Tracing](docs/overview.md).