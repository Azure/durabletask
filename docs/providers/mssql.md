# MSSQL Provider

The MSSQL provider uses Microsoft SQL Server or Azure SQL Database as the backend storage for orchestration state. This provider is maintained in a separate repository.

## Repository

The MSSQL provider is available at: **[https://github.com/microsoft/durabletask-mssql](https://github.com/microsoft/durabletask-mssql)**

## Features

- Uses SQL Server or Azure SQL Database for durable storage
- Supports both on-premises SQL Server and Azure SQL
- Includes database migrations for schema management
- Compatible with DTFx and Azure Durable Functions

## Getting Started

For installation, configuration, and usage documentation, see the [durabletask-mssql repository](https://github.com/microsoft/durabletask-mssql).

## When to Use

Consider the MSSQL provider when:

- You have existing SQL Server infrastructure
- You need the transactional guarantees of a relational database
- You want to query orchestration state using familiar SQL tools
- Your organization has SQL Server expertise and operational practices

## Next Steps

- [durabletask-mssql Documentation](https://github.com/microsoft/durabletask-mssql#readme)
- [Choosing a Backend](../getting-started/choosing-a-backend.md)
