//  ----------------------------------------------------------------------------------
//  Copyright Microsoft Corporation
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//  http://www.apache.org/licenses/LICENSE-2.0
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  ----------------------------------------------------------------------------------

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
            internal const string Operation = "durabletask.task.operation";
            internal const string ScheduledTime = "durabletask.task.scheduled_time";
            internal const string ErrorMessage = "durabletask.entity.error_message";
        }

        internal static class Status
        {
            internal const string Code = "otel.status_code";
            internal const string Description = "otel.status_description";
        }
    }
}
