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

namespace DurableTask.Samples.Replat
{
    using System.Collections.ObjectModel;

    public class MigrateOrchestrationStatus
    {
        public MigrateOrchestrationStatus()
        {
            this.ApplicationsMigrated = new Collection<Application>();
            this.ApplicationsFailed = new Collection<Application>();
        }

        public bool TtlUpdated { get; set; }
        public bool TtlUpdateTimerFired { get; set; }
        public int TotalApplication { get; set; }
        public bool IsMigrated { get; set; }
        public bool IsFlipped { get; set; }
        public bool IsWhitelisted { get; set; }
        public bool IsCleaned { get; set; }

        public bool IsSuccess
        {
            get
            {
                return this.IsMigrated && this.IsFlipped && this.IsWhitelisted;
            }
        }

        public Collection<Application> ApplicationsMigrated { get; private set; }
        public Collection<Application> ApplicationsFailed { get; private set; }
    }
}
