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
            var table = _tableClient.GetTableReference("dummy");

            // Act
            _migrator.CreateMigration(_ =>
            {
                var entity = new DummyEntity {PartitionKey = "dummy", RowKey = rowKey, Name = "dummy"};

                _.Insert("dummy", entity, true);
            }, 1, "1.0");
            
            var query = new TableQuery<DummyEntity>().Where(
                TableQuery.CombineFilters(
                    TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "dummy"),
                    TableOperators.And, TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey)));

            var result = table.ExecuteQuery(query);

            // Assert
            result.FirstOrDefault().Should().NotBeNull();
        }

        [Test]
        public void MigratorTests_WhenTableDeletionIsOrdered_ThenItShouldBeDeletedIfExists()
        {
            // Arrange
            var tableName = "tableToDelete";
            var table = _tableClient.GetTableReference(tableName);

            // Act
            table.CreateIfNotExists();
            _migrator.CreateMigration(_ =>
            {
                _.DeleteTable(tableName);
            }, 1, "1.0");

            // Assert
            table.Exists().Should().BeFalse("It has been deleted in this run.");
        }

        [Test]
        public void MigratorTests_WhenTableCreationIsOrdered_ThenItShouldBeCreatedIfNotExist()
        {
            // Arrange
            var tableName = "tableToCreate";
            var table = _tableClient.GetTableReference(tableName);

            // Act
            table.CreateIfNotExists();
            _migrator.CreateMigration(_ =>
            {
                _.CreateTable(tableName);
            }, 1, "1.0");

            // Assert
            table.Exists().Should().BeTrue("It has been created in this run.");
        }

        [Test]
        public void MigratorTests_WhenRenameTableIsOrdered_ThenNewTableShouldBeCreatedAndDataMoved()
        {
            // Arrange
            var originTable = "origin";
            var destinationTable = "destination";
            var originTableRef = _tableClient.GetTableReference(originTable);
            var destinationTableRef = _tableClient.GetTableReference(destinationTable);
            originTableRef.DeleteIfExists();
            destinationTableRef.DeleteIfExists();
            originTableRef.CreateIfNotExists();

            for (var i = 0; i < 1000; i++)
            {
                var op = TableOperation.Insert(new DummyEntity()
                {
                    Name = $"Foo{i}",
                    PartitionKey = "dummy",
                    RowKey = DateTime.UtcNow.Ticks.ToString()
                });

                originTableRef.Execute(op);
            }

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.RenameTable<DummyEntity>(originTable, destinationTable);
            }, 1, "1.1");

            var query = new TableQuery<DummyEntity>();
            var result = destinationTableRef.ExecuteQuery(query);

            // Assert
            originTableRef.Exists().Should().BeTrue("It shouldn't be deleted after renaming.");
            destinationTableRef.Exists().Should().BeTrue("It should be created after renaming.");
            result.Count().Should().Be(1000, "1000 items were in the origin table");
        }

        public class DummyEntity : TableEntity
        {
            public string Name { get; set; }
        }
    }
}