using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage
{
    /// <summary>
    /// Provides helper functions to work with control-queues.
    /// </summary>
    public interface IControlQueueHelper
    {
        /// <summary>
        /// Gets instanceId which is targeted for mentioned control-queue names.
        /// </summary>
        /// <param name="controlQueueNames">Collection of controlQueueNames.</param>
        /// <param name="instanceIdPrefix">InstanceId prefix.</param>
        /// <returns>InstanceId for control-queue.</returns>
        string GetControlQueueInstanceId(HashSet<string> controlQueueNames, string instanceIdPrefix = "");

        /// <summary>
        /// Gets instanceId which is targeted for mentioned control-queue numbers.
        /// </summary>
        /// <param name="controlQueueNumbers">Collection of controlQueueNumbers.</param>
        /// <param name="instanceIdPrefix">InstanceId prefix.</param>
        /// <returns>InstanceId for control-queue.</returns>
        string GetControlQueueInstanceId(HashSet<int> controlQueueNumbers, string instanceIdPrefix = "");
    }
}
