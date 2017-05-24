using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageMigrator
{
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

        /// <summary>
        /// Deletes an exact entity from a table
        /// </summary>
        public void Delete<T>(string tableName, T entity) where T : TableEntity
        {
            var table = _tableClient.GetTableReference(tableName);
            var op = TableOperation.Delete(entity);

            table.Execute(op);
        }

        /// <summary>
        /// Deletes all entities from a table which have
        /// a specific partition key
        /// </summary>
        public void Delete(string tableName, string partitionKey)
        {
            var table = _tableClient.GetTableReference(tableName);
            var query = new TableQuery().Where(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey));

            foreach (var entity in table.ExecuteQuery(query))
            {
                var op = TableOperation.Delete(entity);
                table.Execute(op);
            }         
        }

        /// <summary>
        /// Deletes all entities from a table which have
        /// a specific partition key and a row key
        /// </summary>
        public void Delete(string tableName, string partitionKey, string rowKey)
        {
            var table = _tableClient.GetTableReference(tableName);
            var query = new TableQuery().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                    TableOperators.And, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey)));

            foreach (var entity in table.ExecuteQuery(query))
            {
                var op = TableOperation.Delete(entity);
                table.Execute(op);
            }
        }

        /// <summary>
        /// Deletes all rows from a table. Useful especially
        /// if you don't want to wait for a table to be
        /// garbage collected.
        /// </summary>
        public void Clear(string tableName)
        {
            var table = _tableClient.GetTableReference(tableName);
            var query = new TableQuery();

            foreach (var entity in table.ExecuteQuery(query))
            {
                var op = TableOperation.Delete(entity);
                table.Execute(op);
            }
        }
    }
}