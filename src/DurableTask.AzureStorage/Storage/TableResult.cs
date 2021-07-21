using System;
using System.Collections.Generic;
using System.Text;

namespace DurableTask.AzureStorage.Storage
{
    class TableResult
    {
        private Microsoft.WindowsAzure.Storage.Table.TableResult storageTableResult;

        public TableResult(Microsoft.WindowsAzure.Storage.Table.TableResult storageTableResult)
        {
            this.storageTableResult = storageTableResult;
        }

        public object Result => this.storageTableResult.Result;
    }
}
