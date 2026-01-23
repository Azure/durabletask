# Support

This document describes support options for the Durable Task Framework (DTFx).

## Community Support (Open Source)

The Durable Task Framework is an open-source project maintained by Microsoft. Community support is available through [GitHub Issues](https://github.com/Azure/durabletask/issues) for bug reports, feature requests, and technical questions.

### Support Policy

Community support is provided on a **best-effort basis**:

- ⚠️ **No SLA** — Response times are not guaranteed
- ⚠️ **No 24/7 coverage** — Issues are triaged during business hours
- ⚠️ **Community-driven** — Many answers come from community members
- ⚠️ **Not all providers maintained** — Some backend providers are no longer actively maintained
- ✅ **Open collaboration** — All issues and discussions are public

See [Choosing a Backend](getting-started/choosing-a-backend.md) for information on the development status of each backend provider.

This model works well for:

- Learning and experimentation
- Non-critical workloads
- Development and testing environments
- Projects with in-house expertise

## Enterprise Support: Durable Task Scheduler

For production workloads requiring guaranteed support, we recommend using DTFx with the **[Durable Task Scheduler](providers/durable-task-scheduler.md)** as the backend provider. This fully managed Azure service offers enterprise-grade support with the following benefits:

| Feature | Open Source (BYO Providers) | Durable Task Scheduler |
| ------- | -------------------------- | ---------------------- |
| **Support SLA** | ❌ Best-effort | ✅ Azure support with SLA |
| **24/7 Coverage** | ❌ No | ✅ Yes (with Azure support plan) |
| **Infrastructure** | Self-managed | Fully managed by Azure |
| **Monitoring** | Bring your own tools | Built-in dashboard |
| **Throughput** | Varies by provider | Highest available |
| **Response Time** | Not guaranteed | Based on Azure support tier |

## Reporting Security Issues

⚠️ **Do not report security vulnerabilities through public GitHub issues.**

Please see [SECURITY.md](../SECURITY.md) for instructions on reporting security issues responsibly.

## Contributing

We welcome contributions! See the [GitHub repository](https://github.com/Azure/durabletask) for contribution guidelines.
