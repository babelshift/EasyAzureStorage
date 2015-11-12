using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public abstract class BaseAzureStorage
    {
        private string connectionString;

        protected BaseAzureStorage(string connectionString)
        {
            this.connectionString = connectionString;
        }

        protected CloudBlobContainer GetContainer(string containerName)
        {
            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            return container;
        }

        /// <summary>
        /// Deletes a blob from the specified container.
        /// </summary>
        /// <param name="containerName">The container to delete the blob from.</param>
        /// <param name="blobName">The name of the blob to delete.</param>
        /// <returns></returns>
        public async Task DeleteBlobAsync(string containerName, string blobName)
        {
            var container = GetContainer(containerName);

            var blob = container.GetBlockBlobReference(blobName);

            await blob.DeleteAsync();
        }
    }
}