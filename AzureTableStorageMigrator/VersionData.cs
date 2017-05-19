using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageMigrator
{
    internal class VersionData : TableEntity
    {
        public int Version { get; set; }

        public string VersionReadable { get; set; }
    }
}