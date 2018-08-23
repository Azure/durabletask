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

namespace DurableTask.AzureStorage.Monitoring
{
    using System;

    /// <summary>
    /// Represents a scale recommendation for the task hub given the current performance metrics.
    /// </summary>
    public class ScaleRecommendation : EventArgs
    {
        internal ScaleRecommendation(ScaleAction scaleAction, bool keepWorkersAlive, string reason)
        {
            this.Action = scaleAction;
            this.KeepWorkersAlive = keepWorkersAlive;
            this.Reason = reason;
        }

        /// <summary>
        /// Gets the recommended scale action for the current task hub.
        /// </summary>
        public ScaleAction Action { get; }

        /// <summary>
        /// Gets a recommendation about whether to keep existing task hub workers alive.
        /// </summary>
        public bool KeepWorkersAlive { get; }

        /// <summary>
        /// Gets text describing why a particular scale action was recommended.
        /// </summary>
        public string Reason { get; }

        /// <summary>
        /// Gets a string description of the current <see cref="ScaleRecommendation"/> object.
        /// </summary>
        /// <returns>A string description useful for diagnostics.</returns>
        public override string ToString()
        {
            return $"{nameof(this.Action)}: {this.Action}, {nameof(KeepWorkersAlive)}: {this.KeepWorkersAlive}, {nameof(Reason)}: {this.Reason}";
        }
    }
}
