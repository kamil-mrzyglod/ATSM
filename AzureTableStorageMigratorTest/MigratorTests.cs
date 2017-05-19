using System;
using System.Configuration;
using System.Linq;
using AzureTableStorageMigrator;
using FluentAssertions;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using NUnit.Framework;

namespace AzureTableStorageMigratorTest
{
    public class MigratorTests
    {
        private Migrator _migrator;
        private CloudTableClient _tableClient;

        [SetUp]
        public void Setup()
        {
            _migrator = new Migrator();

            var cloudStorage = CloudStorageAccount.Parse(
                ConfigurationManager.AppSettings["StorageConnectionString"]);
            _tableClient = cloudStorage.CreateCloudTableClient();
        }

        [Test]
        public void MigratorTests_WhenInsertIsUsedWithAValidTableNameWhichDoesNotExistsAndShouldBeCreated_ThenPassedEntityIsCreated()
        {
            // Arrange
            var rowKey = DateTime.Now.Ticks.ToString();
            var entity = new DummyEntity {PartitionKey = "dummy", RowKey = rowKey, Name = "dummy"};
            var table = _tableClient.GetTableReference("dummy");
            table.DeleteIfExists();

            // Act
            _migrator.Insert("dummy", entity, true);
            
            var query = new TableQuery<DummyEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "dummy"),
                    TableOperators.And, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey)));

            var result = table.ExecuteQuery(query);

            // Assert
            result.FirstOrDefault().Should().NotBeNull();
        }

        public class DummyEntity : TableEntity
        {
            public string Name { get; set; }
        }
    }
}