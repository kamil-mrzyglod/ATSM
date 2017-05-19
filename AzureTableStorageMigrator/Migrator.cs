using System;
using System.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageMigrator
{
    public class Migrator
    {
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

        /// <summary>
        /// Lets you create a new migration chaining available operations
        /// </summary>
        public Migrator CreateMigration(Action<MigratorSyntax> syntax, int version, string versionReadable)
        {
            syntax(new MigratorSyntax(TableClient));
            SaveMigrationData(version, versionReadable);

            return this;
        }

        private void SaveMigrationData(int version, string versionReadable)
        {
            var syntax = new MigratorSyntax(_tableClient);

            syntax.Insert("versionData",
                new VersionData {PartitionKey = "versionData", RowKey = DateTime.UtcNow.Ticks.ToString(), Version = version, VersionReadable = versionReadable},
                true);
        }
    }

    public class MigratorSyntax
    {
        private readonly CloudTableClient _tableClient;

        internal MigratorSyntax() {}

        internal MigratorSyntax(CloudTableClient tableClient)
        {
            _tableClient = tableClient;
        }

        /// <summary>
        /// Inserts an entity using an 'Insert' operation.
        /// </summary>
        public void Insert<T>(string tableName, T entity, bool createIfNotExists = false) where T : TableEntity
        {
            var table = _tableClient.GetTableReference(tableName);
            var op = TableOperation.Insert(entity);

            if (createIfNotExists) table.CreateIfNotExists();
            table.Execute(op);
        }

        /// <summary>
        /// Deletes a table if exists
        /// </summary>
        /// <param name="tableName"></param>
        public void DeleteTable(string tableName)
        {
            var table = _tableClient.GetTableReference(tableName);
            table.DeleteIfExists();
        }
    }
}