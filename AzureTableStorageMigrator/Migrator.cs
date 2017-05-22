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
        public void DeleteTable(string tableName)
        {
            var table = _tableClient.GetTableReference(tableName);
            table.DeleteIfExists();
        }

        /// <summary>
        /// Creates a table if doesn't exist
        /// </summary>
        public void CreateTable(string tableName)
        {
            var table = _tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();
        }

        /// <summary>
        /// Renames origin table to the destination table. Note
        /// that it requires moving all records from the old table
        /// to the new one so all metadata is lost in result. Additionally
        /// it can take a while to move all the data so be patient.
        /// </summary>
        public void RenameTable<T>(string originTable, string destinationTable) where T : TableEntity, new()
        {
            CreateTable(destinationTable);

            var query = new TableQuery<T>();
            var originTableRef = _tableClient.GetTableReference(originTable);
            var destinationTableRef = _tableClient.GetTableReference(destinationTable);
            foreach (var item in originTableRef.ExecuteQuery(query))
            {
                var op = TableOperation.Insert(item);
                destinationTableRef.Execute(op);
            }
        }
    }
}