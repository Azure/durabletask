using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.Core.Tracing
{
    internal static class Schema
    {
        internal static class Task
        {
            internal const string Type = "durabletask.type";
            internal const string Name = "durabletask.task.name";
            internal const string Version = "durabletask.task.version";
            internal const string InstanceId = "durabletask.task.instance_id";
            internal const string ExecutionId = "durabletask.task.execution_id";
            internal const string Status = "durabletask.task.status";
            internal const string TaskId = "durabletask.task.task_id";
            internal const string EventTargetInstanceId = "durabletask.event.target_instance_id";
            internal const string FireAt = "durabletask.fire_at";
        }

        internal static class Status
        {
            internal const string Code = "otel.status_code";
            internal const string Description = "otel.status_description";
        }
    }
}
