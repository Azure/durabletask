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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading.Tasks;
    using DurableTask.Core;

    /// <summary>
    /// </summary>
    public class MigrateOrchestration : TaskOrchestration<bool, MigrateOrchestrationData, string, MigrateOrchestrationStatus>
    {
        MigrateOrchestrationStatus status;
        IMigrationTasks antaresReplatMigrationTasks;
        IManagementSqlOrchestrationTasks managementDatabaseTasks;

        readonly RetryOptions retryOptions = new RetryOptions(TimeSpan.FromSeconds(30), 5)
        {
            BackoffCoefficient = 1,
            MaxRetryInterval = TimeSpan.FromMinutes(5),
        };

        public OrchestrationContext Context { get; set; }

        public override async Task<bool> RunTask(OrchestrationContext context, MigrateOrchestrationData input)
        {
            Initialize(context);

            string subscriptionId = input.SubscriptionId;
            // Only update ttl for enabled subscription
            if (!input.IsDisabled)
            {
                this.status.TtlUpdated = await this.antaresReplatMigrationTasks.UpdateTtl(subscriptionId);
                //this.LogOrchestrationEvent(TraceEventType.Information, "Updated Websites ttl for Subscription '{0}' with result '{1}'".FormatInvariant(subscriptionId,
                //    this.status.TtlUpdated.ToString()));

                // Wait for 1 hour (after TTL update) before starting actual migration to guarantee zero downtime for website
                this.status.TtlUpdateTimerFired = await Context.CreateTimer(Context.CurrentUtcDateTime.AddSeconds(10), true);
            }

            bool subscriptionLocked = await LockSubscription(input.SubscriptionId);
            //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' locked result: {1}".FormatInvariant(subscriptionId, subscriptionLocked.ToString()));

            Application[] apps = await this.managementDatabaseTasks.GetApplicationNames(subscriptionId);
            //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' has '{1}' apps to migrate".FormatInvariant(subscriptionId, apps.Length.ToString()));

            try
            {
                if (subscriptionLocked)
                {
                    if (Validate())
                    {
                        if (input.IsDisabled)
                        {
                            //this.LogOrchestrationEvent(TraceEventType.Information, "Enabling Subscription '{0}' before migration".FormatInvariant(subscriptionId));
                            await this.antaresReplatMigrationTasks.EnableSubscription(input.SubscriptionId);
                        }

                        //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' validated for starting migration".FormatInvariant(subscriptionId));

                        this.status.TotalApplication = apps.Length;
                        this.status.IsMigrated = await ApplyAction(apps, (id, app) => MigrateApplication(id, input.SubscriptionId, app));
                        //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' migration result: {1}".FormatInvariant(subscriptionId,
                        //    this.status.IsMigrated));

                        if (this.status.IsMigrated)
                        {
                            this.status.ApplicationsMigrated.Clear();

                            // All Apps redeployed now switch DNS hostname
                            this.status.IsFlipped = await ApplyAction(apps, (i, app) => this.antaresReplatMigrationTasks.UpdateWebSiteHostName(subscriptionId, app));
                            //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' flipped result: {1}".FormatInvariant(subscriptionId,
                            //    this.status.IsFlipped));
                        }

                        if (this.status.IsFlipped)
                        {
                            this.status.IsWhitelisted = await SafeTaskInvoke(() => this.antaresReplatMigrationTasks.WhitelistSubscription(subscriptionId));
                            //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' IsWhitelisted result: {1}".FormatInvariant(subscriptionId,
                            //    this.status.IsWhitelisted));
                        }

                        if (!this.status.IsSuccess)
                        {
                            // TODO: Cleanup all deployed apps (Rollback)
                        }
                    }
                }
            }
            catch (Exception)
            {
                //this.LogOrchestrationEvent(TraceEventType.Error, "Failed to Migrate Subscription '{0}'".FormatInvariant(
                //    subscriptionId), e.ToString());
            }

            if (subscriptionLocked)
            {
                if (input.IsDisabled)
                {
                    //this.LogOrchestrationEvent(TraceEventType.Information, "Disable Subscription '{0}' after migration".FormatInvariant(subscriptionId));
                    await this.antaresReplatMigrationTasks.DisableSubscription(input.SubscriptionId);
                }

                // Unlock subscription
                await this.managementDatabaseTasks.UpsertSubscriptionLock(subscriptionId, isLocked: false);
                //this.LogOrchestrationEvent(TraceEventType.Information, "Subscription '{0}' Unlocked".FormatInvariant(subscriptionId));
            }

            if (this.status.IsSuccess)
            {
                // wait 5 minutes before cleaning up private stamps
                await Context.CreateTimer(Context.CurrentUtcDateTime.AddSeconds(5), true);

                this.status.IsCleaned = await SafeTaskInvoke(() => this.antaresReplatMigrationTasks.CleanupPrivateStamp(subscriptionId));
                //this.LogOrchestrationEvent(TraceEventType.Information, "Private stamp cleaned for Subscription '{0}', Result: {1}".FormatInvariant(
                //    subscriptionId, this.status.IsCleaned));
            }

            //this.LogOrchestrationEvent(TraceEventType.Information, "Migration result for Subscription '{0}': {1}".FormatInvariant(
            //            subscriptionId, this.status.IsSuccess));
            return this.status.IsSuccess;
        }

        public override MigrateOrchestrationStatus OnGetStatus()
        {
            return this.status;
        }

        void Initialize(OrchestrationContext context)
        {
            Context = context;
            this.status = new MigrateOrchestrationStatus();
            this.antaresReplatMigrationTasks = context.CreateRetryableClient<IMigrationTasks>(this.retryOptions);
            this.managementDatabaseTasks = context.CreateRetryableClient<IManagementSqlOrchestrationTasks>(this.retryOptions);
        }

        //private bool Validate(Application[] apps)
        static bool Validate()
        {
            // TODO: Validate if the entire subscription can be migrated
            //      1) No Apps are Kudu enabled

            return true;
        }

        async Task<bool> LockSubscription(string subscriptionId)
        {
            var subscriptionLocked = false;
            var error = false;
            try
            {
                subscriptionLocked = await this.managementDatabaseTasks.UpsertSubscriptionLock(subscriptionId, isLocked: true);
            }
            catch
            {
                error = true;
            }

            if (error)
            {
                await this.managementDatabaseTasks.UpsertSubscriptionLock(subscriptionId, isLocked: false);
            }

            return subscriptionLocked;
        }

        async Task<bool> MigrateApplication(int id, string subscriptionId, Application application)
        {
            string migrateId = Context.OrchestrationInstance.InstanceId + "-" + id.ToString();
            bool isSuccess = await this.antaresReplatMigrationTasks.ExportSite(migrateId, subscriptionId, application);
            if (isSuccess)
            {
                isSuccess = await this.antaresReplatMigrationTasks.ImportSite(migrateId, subscriptionId, application);
            }

            if (isSuccess)
            {
                isSuccess = await this.antaresReplatMigrationTasks.MigrateServerFarmConfig(migrateId, subscriptionId, application);
            }

            return isSuccess;
        }

        async Task<T> SafeTaskInvoke<T>(Func<Task<T>> task)
        {
            try
            {
                return await task();
            }
            catch
            {
                // Eat up any exception
            }

            return default(T);
        }

        async Task<bool> ApplyAction(Application[] apps, Func<int, Application, Task<bool>> action)
        {
            var actionResults = new List<bool>();
            int totalApplications = apps.Length;
            for (var i = 0; i < totalApplications; i++)
            {
                Application app = apps[i];
                bool actionResult = await SafeTaskInvoke(() => action(i, app));
                if (actionResult)
                {
                    this.status.ApplicationsMigrated.Add(app);
                }
                else
                {
                    this.status.ApplicationsFailed.Add(app);
                }

                actionResults.Add(actionResult);
            }

            bool[] results = actionResults.ToArray();
            bool isSuccess = results.All(r => r);
            return isSuccess;
        }
    }
}
