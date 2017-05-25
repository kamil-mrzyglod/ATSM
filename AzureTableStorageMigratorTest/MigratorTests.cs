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

            _tableClient.GetTableReference("versionData").DeleteIfExists();
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
            }, 1, "1.0", "MigratorTests_WhenInsertIsUsedWithAValidTableNameWhichDoesNotExistsAndShouldBeCreated_ThenPassedEntityIsCreated");
            
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
            }, 2, "1.0", "MigratorTests_WhenTableDeletionIsOrdered_ThenItShouldBeDeletedIfExists");

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
            }, 3, "1.0", "MigratorTests_WhenTableCreationIsOrdered_ThenItShouldBeCreatedIfNotExist");

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
                _.RenameTable(originTable, destinationTable);
            }, 4, "1.1", "MigratorTests_WhenRenameTableIsOrdered_ThenNewTableShouldBeCreatedAndDataMoved");

            var query = new TableQuery<DummyEntity>();
            var result = destinationTableRef.ExecuteQuery(query);

            // Assert
            originTableRef.Exists().Should().BeTrue("It shouldn't be deleted after renaming.");
            destinationTableRef.Exists().Should().BeTrue("It should be created after renaming.");
            result.Count().Should().Be(1000, "1000 items were in the origin table");
        }

        [Test]
        public void MigratorTests_WhenAnEntityIsDeleted_ItIsNoLongerInATable()
        {
            // Arrange
            var tableName = "deleting";
            var entity = new DummyEntity {PartitionKey = "PK", RowKey = "RK", ETag = "*"};
            var tableRef = _tableClient.GetTableReference(tableName);
            tableRef.DeleteIfExists();
            tableRef.CreateIfNotExists();
            var op = TableOperation.Insert(entity);
            tableRef.Execute(op);

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.Delete(tableName, entity);
            }, 5, "1.2", "MigratorTests_WhenAnEntityIsDeleted_ItIsNoLongerInATable");

            // Assert
            var query = new TableQuery<DummyEntity>();
            var result = tableRef.ExecuteQuery(query);
            result.FirstOrDefault(e => e.RowKey == "PK").Should().BeNull("this entity was deleted.");
        }

        [Test]
        public void MigratorTests_WhenAnEntityWithASpecificPartitionKeyIsDeleted_ItIsNoLongerInATable()
        {
            // Arrange
            var tableName = "deleting2";
            var entity = new DummyEntity { PartitionKey = "PK", RowKey = "RK", ETag = "*" };
            var entity2 = new DummyEntity { PartitionKey = "PK2", RowKey = DateTime.UtcNow.Ticks.ToString(), ETag = "*" };
            var tableRef = _tableClient.GetTableReference(tableName);
            tableRef.DeleteIfExists();
            tableRef.CreateIfNotExists();
            var op = TableOperation.Insert(entity);
            var op2 = TableOperation.Insert(entity2);
            tableRef.Execute(op);
            tableRef.Execute(op2);

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.Delete(tableName, "PK");
            }, 6, "1.2", "MigratorTests_WhenAnEntityWithASpecificPartitionKeyIsDeleted_ItIsNoLongerInATable");

            // Assert
            var query = new TableQuery<DummyEntity>();
            var result = tableRef.ExecuteQuery(query).ToList();
            result.FirstOrDefault(e => e.PartitionKey == "PK").Should().BeNull("this entity was deleted.");
            result.FirstOrDefault(e => e.PartitionKey == "PK2").Should().NotBeNull("this entity has a diferent PK.");
        }

        [Test]
        public void MigratorTests_WhenAnEntityWithASpecificPartitionKeyAndARowKeyIsDeleted_ItIsNoLongerInATable()
        {
            // Arrange
            var tableName = "deleting3";
            var entity = new DummyEntity { PartitionKey = "PK", RowKey = "RK", ETag = "*" };
            var entity2 = new DummyEntity { PartitionKey = "PK2", RowKey = "RK2", ETag = "*" };
            var entity3 = new DummyEntity { PartitionKey = "PK3", RowKey = "RK3", ETag = "*" };
            var tableRef = _tableClient.GetTableReference(tableName);
            tableRef.DeleteIfExists();
            tableRef.CreateIfNotExists();
            var op = TableOperation.Insert(entity);
            var op2 = TableOperation.Insert(entity2);
            var op3 = TableOperation.Insert(entity3);
            tableRef.Execute(op);
            tableRef.Execute(op2);
            tableRef.Execute(op3);

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.Delete(tableName, "PK", "RK");
            }, 7, "1.3", "MigratorTests_WhenAnEntityWithASpecificPartitionKeyAndARowKeyIsDeleted_ItIsNoLongerInATable");

            // Assert
            var query = new TableQuery<DummyEntity>();
            var result = tableRef.ExecuteQuery(query).ToList();
            result.FirstOrDefault(e => e.PartitionKey == "PK" && e.RowKey == "RK").Should().BeNull("this entity was deleted.");
            result.FirstOrDefault(e => e.PartitionKey == "PK2" && e.RowKey == "RK2").Should().NotBeNull("this entity has a diferent PK and RK.");
            result.FirstOrDefault(e => e.PartitionKey == "PK3" && e.RowKey == "RK3").Should().NotBeNull("this entity has a diferent PK and RK.");
        }

        [Test]
        public void MigratorTests_WhenTableIsCleared_ThenNoRowIsInsideIt()
        {
            // Arrange
            var tableName = "clearing";
            var table = _tableClient.GetTableReference(tableName);
            table.CreateIfNotExists();

            for (var i = 0; i < 100; i++)
            {
                var op = TableOperation.Insert(new DummyEntity
                {
                    PartitionKey = "clearing",
                    RowKey = DateTime.UtcNow.Ticks.ToString(),
                    Name = i.ToString()
                });

                table.Execute(op);
            }

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.Clear(tableName);
            }, 8, "1.4", "MigratorTests_WhenTableIsCleared_ThenNoRowIsInsideIt");

            var query = new TableQuery();
            var result = table.ExecuteQuery(query);

            // Assert
            result.Count().Should().Be(0, "we just cleared all records");
        }

        [Test]
        public void MigratorTests_WhenDuplicatedMigrationIdIsUsed_ThenMigratorIsStopped()
        {
            // Arrange

            // Act
            void Act() => _migrator.CreateMigration(_ => { _.CreateTable("duplicated1"); }, 9, "1.0",
                    "MigratorTests_WhenDuplicatedMigrationIdIsUsed_ThenMigratorIsStopped")
                .CreateMigration(_ => { _.CreateTable("duplicated2"); }, 9, "1.1",
                    "MigratorTests_WhenDuplicatedMigrationIdIsUsed_ThenMigratorIsStopped");

            // Assert
            Assert.Throws<DuplicatedMigrationException>(Act);
        }

        [Test]
        public void MigratorTests_WhenAComplexMigrationIsCalled_ThenItIsExecutedFlawlessly()
        {
            // Arrange
            var table1 = _tableClient.GetTableReference("complex1");
            table1.DeleteIfExists();
            var table2 = _tableClient.GetTableReference("complex2");
            table2.DeleteIfExists();

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.CreateTable("complex1");
                _.CreateTable("complex2");
                _.Insert("complex1", new DummyEntity { PartitionKey = "complex", RowKey = DateTime.UtcNow.Ticks.ToString(), Name = "foo"});
                _.Insert("complex1", new DummyEntity { PartitionKey = "complex", RowKey = DateTime.UtcNow.Ticks.ToString(), Name = "foo2"});
                _.Insert("complex2", new DummyEntity { PartitionKey = "complex", RowKey = DateTime.UtcNow.Ticks.ToString(), Name = "foo"});
            }, 11, "1.11", "MigratorTests_WhenAComplexMigrationIsCalled_ThenItIsExecutedFlawlessly");

            var query = new TableQuery();
            var result1 = table1.ExecuteQuery(query);
            var result2 = table2.ExecuteQuery(query);

            // Assert
            table1.Exists().Should().BeTrue("we created it in our migration");
            table2.Exists().Should().BeTrue("we created it in our migration");
            result1.Count().Should().Be(2, "we added 2 entities");
            result2.Count().Should().Be(1, "we added 1 entity");
        }

        [Test]
        public void MigratorTests_WhenADeleteColumnIsCalledWithProperParameters_ThenAColumnShouldNoLongerBeInATable()
        {
            // Arrange
            var tableName = "deletingColumn";
            var tableNameTemp = "deletingColumnTEMP";
            var tableRef = _tableClient.GetTableReference(tableName);
            var tableTempRef = _tableClient.GetTableReference(tableNameTemp);
            tableRef.DeleteIfExists();
            tableTempRef.DeleteIfExists();
            tableRef.CreateIfNotExists();
            var op = TableOperation.Insert(new DummyEntityWithAColumn
            {
                PartitionKey = "dummy",
                RowKey = DateTime.UtcNow.Ticks.ToString(),
                Name = "Entity1",
                Dummy = "HAHA"
            });
            var op2 = TableOperation.Insert(new DummyEntityWithAColumn
            {
                PartitionKey = "dummy2",
                RowKey = DateTime.UtcNow.Ticks.ToString(),
                Name = "Entity2",
                Dummy = "HOHO"
            });
            var op3 = TableOperation.Insert(new DummyEntityWithAColumn
            {
                PartitionKey = "dummy3",
                RowKey = DateTime.UtcNow.Ticks.ToString(),
                Name = "Entity3",
                Dummy = "HIHI"
            });

            tableRef.Execute(op);
            tableRef.Execute(op2);
            tableRef.Execute(op3);

            // Act
            _migrator.CreateMigration(_ =>
            {
                _.DeleteColumn<DummyEntityWithAColumn, DummyEntity>(tableName);
            }, 12, "1.12", "MigratorTests_WhenADeleteColumnIsCalledWithProperParameters_ThenAColumnShouldNoLongerBeInATable");

            var query = new TableQuery<DummyEntityWithAColumn>();
            var result = tableRef.ExecuteQuery(query).ToList();

            // Assert
            tableRef.Exists().Should().BeTrue("deleting a column mustn't affect a table");
            tableTempRef.Exists().Should().BeFalse("temporary table shouldn't exist after a migration");
            result.Count.Should().Be(3, "we had 3 entities in the table");
            result.All(e => string.IsNullOrEmpty(e.Dummy)).Should().BeTrue("we just changed the type of a table");
        }

        [Test]
        public void MigratorTests_WhenInvalidTypeIsUsedForDeletingAColumn_ThenMigratorIsStopped()
        {
            // Arrange
            var tableName = "deletingColumn2";

            // Act
            void Act() => _migrator.CreateMigration(_ =>
            {
                _.DeleteColumn<DummyEntity, DummyEntityWithAColumn>(tableName);
            }, 13, "1.13", "MigratorTests_WhenInvalidTypeIsUsedForDeletingAColumn_ThenMigratorIsStopped");

            // Assert
            Assert.Throws<InvalidOperationException>(Act);
        }

        public class DummyEntity : TableEntity
        {
            public DummyEntity()
            {
            }

            public DummyEntity(DummyEntityWithAColumn entity)
            {
                PartitionKey = entity.PartitionKey;
                RowKey = entity.RowKey;
                Name = entity.Name;
            }

            public string Name { get; set; }
        }

        public class DummyEntityWithAColumn : TableEntity
        {
            public string Name { get; set; }

            public string Dummy { get; set; }
        }
    }
}