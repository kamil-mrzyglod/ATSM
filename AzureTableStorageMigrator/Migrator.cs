using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageMigrator
{
    public class Migrator
    {
        private static readonly IList<int> UsedIds = new List<int>();

        private readonly CloudStorageAccount _cloudStorage;

        private CloudTableClient _tableClient;

        /// <summary>
        /// Creates an instance of Migrator using a default('StorageConnectionString')
        /// connection string and letting it fetch it by itself
        /// </summary>
        public Migrator()
        {
            _cloudStorage = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings["StorageConnectionString"]);
        }

        /// <summary>
        /// Creates an instance of Migrator by fetching a custom connection
        /// string using a key in application settings
        /// </summary>
        /// <param name="appSettingsName">Application setting key holding a value of a connection string</param>
        public Migrator(string appSettingsName)
        {
            _cloudStorage = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings[appSettingsName]);
        }

        /// <summary>
        /// Creates an instance of Migrator using passed CloudStorageAccount
        /// </summary>
        /// <param name="cloudStorage">Already created instance of CloudStorageAccount</param>
        public Migrator(CloudStorageAccount cloudStorage)
        {
            _cloudStorage = cloudStorage;
        }

        private CloudTableClient TableClient => _tableClient ?? (_tableClient = _cloudStorage.CreateCloudTableClient());

        private IEnumerable<VersionData> SavedMigrations
        {
            get
            {
                var table = TableClient.GetTableReference("versionData");
                if (table.Exists())
                {
                    var query = new TableQuery<VersionData>();
                    var migrations = table.ExecuteQuery(query);

                    return migrations;
                }

                return Enumerable.Empty<VersionData>();
            }
        }

        /// <summary>
        /// Lets you create a new migration chaining available operations
        /// </summary>
        public Migrator CreateMigration(Action<MigratorSyntax> syntax, int id, string versionReadable, string description)
        {
            if(UsedIds.Contains(id)) throw new DuplicatedMigrationException("This migration duplicates previously used migration ID.");
            UsedIds.Add(id);

            if (SavedMigrations == null || SavedMigrations.All(m => m.RowKey != id.ToString()))
            {
                syntax(new MigratorSyntax(TableClient));
                SaveMigrationData(id, versionReadable, description);
            }

            return this;
        }

        private void SaveMigrationData(int id, string versionReadable, string description)
        {
            var syntax = new MigratorSyntax(_tableClient);

            syntax.Insert("versionData",
                new VersionData
                {
                    PartitionKey = "versionData",
                    RowKey = id.ToString(),
                    Version = versionReadable,
                    Timestamp = DateTime.UtcNow,
                    Description = description
                },
                true);
        }
    }

    public class DuplicatedMigrationException : Exception
    {
        public DuplicatedMigrationException(string message)
            : base(message)
        {
        }
    }
}