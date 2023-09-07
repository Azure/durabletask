# DurableTask Distributed Tracing Specification
**Status**: [Experimental](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/document-status.md)

## Overview
This document serves as a specification for Distributed Tracing in the DurableTask Framework. It includes a list of all spans, their attributes, and detailed information about what constitutes each respective span. We want to gather community feedback and encourage any comments with suggestions and questions. 

## Table of Contents
1. [Attributes](#attributes)
2. Spans <br>
    a. [Client: Starting an Orchestration](#client-starting-an-orchestration)<br>
    b. [Client: Starting a sub-orchestration](#client-starting-a-sub-orchestration)<br>
    c. [Worker: Running an Orchestration](#worker-running-an-orchestration)<br>
    d. [Worker: Starting an Activity](#worker-starting-an-activity)<br>
    e. [Worker: Running an Activity](#worker-running-an-activity)<br>
    f. [Worker: Timer](#worker-timer)<br>
    g. [Sending an Event](#sending-an-event)<br>
    
## References
- [Open Telemetry Trace Semantic Conventions](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions "Open Telemetry Trace Semantic Conventions") – various trace semantic conventions we reference throughout this document. Particularly, we pull in some attributes defined in these files.

## Specification
The following only applies to `DurableTask.Core`

### Attributes
These attributes are all new, defined by us, and specific to DurableTask. All attributes here begin with the “durabletask” prefix.

Name | Type | Description | Requirement Level
---|---|---|---
durabletask.type | string | The type of durable event occurring. Options: “activity”, “orchestration”, “timer”, “event” | Required
durabletask.task.name | string | The name of the durable task being invoked. This is the name it was scheduled with, not necessarily the name of the handler class/type. | Required
durabletask.task.version | string | The version of the task being executed.  | Conditionally Required [1]
durabletask.task.instance_id |  string | The instance ID of the task being ran. This is unique for all tasks. | Required [2]
durabletask.task.execution_id | string | The execution ID of the task being ran. This is unique for all tasks. | Required [2]
durabletask.task.task_id | integer | The ID / index of the task within the orchestration. | Conditionally required
durabletask.task.result | string | The terminal status of a task. Options: “Succeeded”, “Failed”, “Terminated” | Required
durabletask.event.name | string | The name of the event being raised. | Required
durabletask.event.instance_id | string | The target orchestration instance ID of a raised event. | Required

[1] durabletask.task.version may be omitted when version is null. 

[2] When instance ID is not available (e.g. activities) use the parent orchestration's instance ID

### Spans
#### Client: Starting an Orchestration

Represents enqueueing a new orchestration via TaskHubClient to the backend.

| Span Property | Span Value |
|---|---|
| Name  | `create_orchestration:{orchestrationName}(@{orchestrationVersion})?`  |
| Kind  | Producer  |
| Start  | Timestamp before submitting the new orchestration to the backend  |
| End  | Timestamp after the orchestration has been submitted to the backend  |
| Status  | OK – successfully enqueued<br />Error – failed to enqueue for any reason  |
| Status Description  | On failure – exception message which caused failure  |
| Links  | None  |
| Events  | TBD  |
| Parent  | Enclosing span |

&nbsp;
Attributes:

| Name  | Value  |
|---|---|
| durabletask.type | "orchestration" |
| durabletask.task.name  | Name of the enqueued orchestration  |
| durabletask.task.version  | Version of the orchestration enqueued. Omitted if null version.  |
| durabletask.task.instance_id  | Instance ID of the enqueued orchestration  |
| durabletask.task.execution_id  | Execution ID of the enqueued orchestration  |
| exception.*  | Exception details on failure. |

#### Client: Starting a sub-orchestration

Represents enqueueing and waiting for a sub-orchestration to complete 

| Span Property | Span Value |
|---|---|
| Name   | `orchestration:{orchestrationName}(@{orchestrationVersion})?`   |
| Kind   | Standard: Client<br />Fire and forget: Producer  |
| Start   | Timestamp of when the parent orchestration enqueues a message to execute the sub\-orchestration  |
| End   | Standard: Timestamp of when the parent orchestration is notified that the sub\-orchestration completed\.<br />Fire and forget: Timestamp after the sub\-orchestration started event is enqueued  |
| Status   | OK – successfully enqueued<br />Error – failed to enqueue for any reason   |
| Links   | None   |
| Events   | TBD   |
| Parent   | “Running an orchestration” span for parent orchestration |

&nbsp;
Attributes:

| Name   | Value   |
|---|---|
| durabletask.type   | “orchestration”   |
| durabletask.task.name   | Name of the enqueued sub-orchestration   |
| durabletask.task.version   | Version of the sub-orchestration enqueued. Omitted if null version.   |
| durabletask.task.instance_id   | Instance ID of the enqueued orchestration   |
| exception.*   | Exception details on failure |


#### Worker: Running an Orchestration

Represents enqueueing a new orchestration via TaskHubClient to the backend

| Span Property | Span Value |
|---|---|
| Name   | `orchestration:{orchestrationName}(@{orchestrationVersion})?`   |
| Kind   | Server   |
| Start   | Timestamp before the orchestration starts executing\.  |
| End   | Timestamp after the orchestration has finished executing\.   |
| Status   | OK – successfully completed<br />Error – failed  |
| Links   | Spans from “Sending an Event”\.  |
| Events   | Received events – name of the event, and timestamp of the first time this event has been played  |
| Parent   | “Starting an orchestration” or “starting a sub\-orchestration” |

&nbsp;
Attributes:

| Name   | Value   |
|---|---|
| durabletask.type   | “orchestration”   |
| durabletask.task.name   | Name of the enqueued orchestration   |
| durabletask.task.version   | Version of the orchestration enqueued. Omitted if null version.   |
| durabletask.task.instance_id   | Instance ID of the enqueued orchestration   |
| Durabletask.task.status  | Orchestration status  |
| exception.*   | Exception details on failure |


#### Worker: Starting an Activity

Represents enqueueing an activity

| Span Property | Span Value |
|---|---|
| Name   | `activity:{activityName}(@{orchestrationVersion})?`   |
| Kind   | Client   |
| Start   | Timestamp before enqueuing the activity  |
| End   | Timestamp after the activity has finished executing\.   |
| Status   | OK – successfully enqueued<br />Error – failed to enqueue for any reason   |
| Links   | None   |
| Events   | TBD   |
| Parent   | “Running an orchestration” span |

&nbsp;
Attributes:

| Name   | Value   |
|---|---|
| durabletask.type   | “activity”   |
| durabletask.task.name   | Name of the enqueued sub-orchestration   |
| durabletask.task.version   | Version of the sub-orchestration enqueued. Omitted if null version.   |
| durabletask.task.instance_id   | Instance ID of the enqueued orchestration   |
| durabletask.task.task_id   | ID of the current task  |
| exception.*   | Exception details on failure |

#### Worker: Running an Activity

Represents the activity executing

| Span Property | Span Value |
|---|---|
| Name   | `activity:{activityName}(@{orchestrationVersion})?`   |
| Kind   | Server   |
| Start   | Timestamp before the activity starts executing\.  |
| End   | Timestamp after the activity has finished executing\.   |
| Status   | OK – successfully completed<br />Error – failed  |
| Links   | None   |
| Events   | TBD   |
| Parent   | “Starting an activity” span |

&nbsp;
Attributes:

| Name   | Value   |
|---|---|
| durabletask.type   | “activity”   |
| durabletask.task.name   | Name of the activity   |
| durabletask.task.version   | Version of the invoking orchestration enqueued. Omitted if null version.   |
| durabletask.task.instance_id   | Instance ID of the invoking orchestration   |
| durabletask.task.task_id   | ID of the current task  |
| exception.*   | Exception details on failure |


#### Worker: Timer

Represents the Durable Timer

| Span Property | Span Value |
|---|---|
| Name    | `orchestration:{orchestrationName}:timer`    |
| Kind    | Internal    |
| Start    | Timestamp of when the timer is created   |
| End    | Timestamp of when the TimerFired event is processed    |
| Status    | OK – timer successfully completed<br />Error – timer cancellation   |
| Links    | None    |
| Events    | TBD    |
| Parent    | “Running an orchestration” span |

&nbsp;
Attributes:

| Name    | Value    |
|---|---|
| durabletask.type    | “timer”    |
| durabletask.fire_at  | Configured FireAt time displayed in ISO 8601 format  |
| durabletask.task.version    | Version of the invoking orchestration enqueued. Omitted if null version.    |
| durabletask.task.instance_id    | Instance ID of the invoking orchestration    |
| durabletask.task.task_id   | ID of the current task |


#### Sending an Event

Represents sending an event to an orchestration

| Span Property | Span Value |
|---|---|
| Name    | `orchestration_event:{orchestration_name}`  |
| Kind    | Producer |
| Start    | Timestamp efore sending the event  |
| End    | From client: Timestamp of when the event has been sent <br />From worker: Timestamp of when the event message has been created \(this has a negligible duration, but we want a span for a link\) |
| Status    | OK – event sent<br />Error – event failed to send  |
| Links    | None    |
| Events    | None  |
| Parent    | Current active span\. For orchestrations, typically the orchestration running span\. For client, whatever span is active or none if no active span\. |

&nbsp;
Attributes:

| Name    | Value    |
|---|---|
| durabletask.type    | “event”    |
| durabletask.event.name  | The name of the event being sent.  |
| durabletask.event.target_instance_id  | The instance ID of the target orchestration (the one receiving the event).    |
| durabletask.task.instance_id    | Instance ID of the orchestration that is sending the event. Not present if sent from the client.  |
| durabletask.task.execution_id   | Execution ID of the orchestration that is sending the event. Not present if sent from the client. |
