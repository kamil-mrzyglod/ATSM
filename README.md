# ATSM
Azure Table Storage Migrator

## Why?
Because even a simple key-value database likes to be versioned.

## Installation
`PM> Install-Package AzureTableStorageMigrator`

## Project
ATSM gives you a simple API, which you can use in your project to decorate each change in Azure Table Storage. What is more, it lets you to actually change the schema of tables and move data. You can create a migration using a following method available in `Migrator` type:

`Migrator CreateMigration(Action<MigratorSyntax> syntax, int id, string versionReadable, string description)`

where `id` is a unique identifier of a migration.

For now following methods are available:
* `void Insert<T>(string tableName, T entity, bool createIfNotExists = false)`
* `void DeleteTable(string tableName)`
* `void CreateTable(string tableName)`
* `void RenameTable<T>(string originTable, string destinationTable)`
* `void Delete<T>(string tableName, T entity)`
* `void Delete(string tableName, string partitionKey)`
* `void Delete(string tableName, string partitionKey, string rowKey)`
* `void Clear(string tableName)`

## Metadata
Each migration leaves a metadata record in the `versionData` table, which will be created in your storage account. Be aware of the fact, that ATSM uses this table internally to decide which migration should be run - you can take advantage of this information if something goes wrong.

## Usage
```
var migrator = new Migrator();
migrator.CreateMigration(_ =>
{
  _.CreateTable("table1");
  _.CreateTable("table2");
  _.Insert("table1", new DummyEntity { PartitionKey = "pk", RowKey = DateTime.UtcNow.Ticks.ToString(), Name = "foo"});
  _.Insert("table1", new DummyEntity { PartitionKey = "pk", RowKey = DateTime.UtcNow.Ticks.ToString(), Name = "foo2"});
  _.Insert("table2", new DummyEntity { PartitionKey = "pk", RowKey = DateTime.UtcNow.Ticks.ToString(), Name = "foo"});
}, 1, "1.1", "My first migration!");
```
