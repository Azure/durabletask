# DurableTask.Samples

This project contains core sample orchestrations demonstrating fundamental patterns of the Durable Task Framework using the Azure Storage backend.

## Prerequisites

- .NET Framework 4.8 or later
- Azure Storage Emulator (Azurite) or Azure Storage account

## Configuration

Configure the connection string in `App.config`:

```xml
<appSettings>
  <add key="StorageConnectionString" value="UseDevelopmentStorage=true" />
  <add key="taskHubName" value="SamplesHub" />
</appSettings>
```

For Azure Storage, replace `UseDevelopmentStorage=true` with your connection string (if not using the emulator):

```text
DefaultEndpointsProtocol=https;AccountName=...;AccountKey=...
```

## Running the Samples

### 1. Create the Task Hub (first time only)

```bash
DurableTask.Samples.exe -c
```

### 2. Start an Orchestration

```bash
DurableTask.Samples.exe -s <OrchestrationName> [-p <parameters>]
```

The worker automatically starts and waits for the orchestration to complete.

## Available Samples

Each sample's full source is linked below. For deeper explanations of the patterns
they demonstrate, see the [DTFx documentation](../../docs/README.md).

### Greetings

Two activities run in sequence: `GetUserTask` produces a name, then `SendGreetingTask`
greets it. A minimal introduction to orchestrations and activities.

- Source: [Greetings/GreetingsOrchestration.cs](Greetings/GreetingsOrchestration.cs)
- Docs: [Orchestrations](../../docs/concepts/orchestrations.md), [Activities](../../docs/concepts/activities.md)
- **Run:** `DurableTask.Samples.exe -s Greetings`

### Greetings2

A variation that races the `GetUserTask` activity against a durable timer using
`Task.WhenAny`, so the orchestration proceeds when the user responds or the timeout
elapses, whichever comes first. The parameter is the timeout in seconds.

- Source: [Greetings2/GreetingsOrchestration.cs](Greetings2/GreetingsOrchestration.cs)
- Docs: [Timers](../../docs/features/timers.md)
- **Run:** `DurableTask.Samples.exe -s Greetings2 -p 5`

### Cron

Schedules `CronTask` to run repeatedly on a [cron](https://en.wikipedia.org/wiki/Cron)
expression (via NCrontab), using `CreateTimer` to wait between runs. The parameter is the
cron schedule and is optional (it falls back to a fixed interval when omitted).

- Source: [Cron/CronOrchestration.cs](Cron/CronOrchestration.cs)
- Docs: [Timers](../../docs/features/timers.md), [Eternal Orchestrations](../../docs/features/eternal-orchestrations.md)
- **Run:** `DurableTask.Samples.exe -s Cron -p "0 12 * */2 Mon"`

### Average

A fan-out/fan-in pattern that splits a numeric range into chunks, computes a partial sum
for each chunk in parallel via `ComputeSumTask`, then aggregates the results into an
average. Parameters are `<start> <end> <step>`.

- Source: [AverageCalculator/AverageCalculatorOrchestration.cs](AverageCalculator/AverageCalculatorOrchestration.cs)
- Docs: [Orchestrations](../../docs/concepts/orchestrations.md)
- **Run:** `DurableTask.Samples.exe -s Average -p "1 50 10"`

### ErrorHandling

Demonstrates exception handling with `try`/`catch` around activities and a compensating
`CleanupTask` when an activity fails.

- Source: [ErrorHandling/ErrorHandlingOrchestration.cs](ErrorHandling/ErrorHandlingOrchestration.cs)
- Docs: [Error Handling](../../docs/features/error-handling.md), [Retries](../../docs/features/retries.md)
- **Run:** `DurableTask.Samples.exe -s ErrorHandling`

### Signal

Demonstrates external events using the `OnEvent` + `TaskCompletionSource` pattern: the
orchestration waits for an external signal and then sends a greeting with the signaled value.

- Source: [Signal/SignalOrchestration.cs](Signal/SignalOrchestration.cs)
- Docs: [External Events](../../docs/features/external-events.md)
- **Run:** `DurableTask.Samples.exe -s Signal`

To raise an event to a running instance:

```bash
DurableTask.Samples.exe -n <eventName> -i <instanceId> -p <eventData>
```

You can also start an instance and raise its first event in one step with the
`SignalAndRaise` sample:

- **Run:** `DurableTask.Samples.exe -s SignalAndRaise -n <eventName> -p <eventData>`

### SumOfSquares

A recursive fan-out/fan-in example that walks a nested JSON array
([BagofNumbers.json](SumOfSquares/BagofNumbers.json)), squaring integers via
`SumOfSquaresTask` and recursing into nested arrays as sub-orchestrations.

- Source: [SumOfSquares/SumOfSquaresOrchestration.cs](SumOfSquares/SumOfSquaresOrchestration.cs)
- Docs: [Sub-Orchestrations](../../docs/features/sub-orchestrations.md)
- **Run:** `DurableTask.Samples.exe -s SumOfSquares`

## Command Line Options

| Option | Description |
| ------ | ----------- |
| `-c` | Create the task hub (required on first run) |
| `-s <name>` | Start the specified orchestration |
| `-p <params>` | Parameters to pass to the orchestration |
| `-i <id>` | Instance ID (auto-generated if not specified) |
| `-n <name>` | Event name for raising events |
| `-w` | Skip the worker (useful when worker runs separately) |

## Additional Resources

- [Getting Started Guide](../../docs/getting-started/quickstart.md)
- [Orchestrations](../../docs/concepts/orchestrations.md)
- [Activities](../../docs/concepts/activities.md)
- [Error Handling](../../docs/features/error-handling.md)
- [Timers](../../docs/features/timers.md)
