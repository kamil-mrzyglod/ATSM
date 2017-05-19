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
        /// Inserts an entity using an 'Insert' operation.
        /// </summary>
        public Migrator Insert<T>(string tableName, T entity, bool createIfNotExists = false) where T : TableEntity
        {
            var table = TableClient.GetTableReference(tableName);
            var op = TableOperation.Insert(entity);

            if (createIfNotExists) table.CreateIfNotExists();

            table.Execute(op);
            return this;
        }
    }
}