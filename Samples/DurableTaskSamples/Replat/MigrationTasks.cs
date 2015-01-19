namespace DurableTaskSamples.Replat
{
    using System;
    using System.Threading.Tasks;

    public interface IMigrationTasks
    {
        Task<bool> ExportSite(string id, string subscriptionId, Application application);
        Task<bool> ImportSite(string id, string subscriptionId, Application application);
        Task<bool> MigrateServerFarmConfig(string id, string subscriptionId, Application application);
        Task<bool> UpdateWebSiteHostName(string subscriptionId, Application application);
        Task<bool> WhitelistSubscription(string subscriptionId);
        Task<bool> CleanupPrivateStamp(string subscriptionId);
        Task<bool> EnableSubscription(string subscriptionId);
        Task<bool> DisableSubscription(string subscriptionId);
        Task<bool> UpdateTtl(string subscriptionId);
    }

    public class MigrationTasks : IMigrationTasks
    {
        public Task<bool> ExportSite(string id, string subscriptionId, Application application)
        {
            Console.WriteLine(string.Format(
                "Export Site Called for SubId: '{0}' with Id: '{1}'.  App Name: '{2}'", subscriptionId, id, application.Name));

            return Task.FromResult(true);
        }

        public Task<bool> ImportSite(string id, string subscriptionId, Application application)
        {
            Console.WriteLine(string.Format(
                "Import Site Called for SubId: '{0}' with Id: '{1}'.  App Name: '{2}'", subscriptionId, id, application.Name));

            return Task.FromResult(true);
        }

        public Task<bool> MigrateServerFarmConfig(string id, string subscriptionId, Application application)
        {
            Console.WriteLine(string.Format(
                "Migrate ServerFarm Called for SubId: '{0}' with Id: '{1}'.  App Name: '{2}'", subscriptionId, id, application.Name));

            return Task.FromResult(true);
        }

        public Task<bool> UpdateWebSiteHostName(string subscriptionId, Application application)
        {
            Console.WriteLine(string.Format(
                "Update Website Hostname Called for SubId: '{0}'.  App Name: '{1}'", subscriptionId, application.Name));

            return Task.FromResult(true);
        }

        public Task<bool> WhitelistSubscription(string subscriptionId)
        {
            Console.WriteLine(string.Format(
                "WhitelistSubscription Called for SubId: '{0}'.", subscriptionId));

            return Task.FromResult(true);
        }

        public Task<bool> CleanupPrivateStamp(string subscriptionId)
        {
            Console.WriteLine(string.Format(
                "CleanupPrivateStamp Called for SubId: '{0}'.", subscriptionId));

            return Task.FromResult(true);
        }

        public Task<bool> EnableSubscription(string subscriptionId)
        {
            throw new NotImplementedException();
        }

        public Task<bool> DisableSubscription(string subscriptionId)
        {
            throw new NotImplementedException();
        }

        public Task<bool> UpdateTtl(string subscriptionId)
        {
            Console.WriteLine(string.Format(
                "UpdateTtl Called for SubId: '{0}'.", subscriptionId));

            return Task.FromResult(true);
        }
    }
}
