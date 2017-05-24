using System;
using Microsoft.WindowsAzure.Storage.Table;

namespace AzureTableStorageMigrator
{
    internal class VersionData : TableEntity
    {
        public string Version { get; set; }

        public DateTime Timestamp { get; set; }

        public string Description { get; set; }
    }
}