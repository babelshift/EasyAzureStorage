using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public class BlobStorageClient
    {
        private CloudStorageAccount storageAccount;
        private CloudBlobClient blobClient;

        public BlobStorageClient(string connectionString)
        {
            if (String.IsNullOrWhiteSpace(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }

            storageAccount = CloudStorageAccount.Parse(connectionString);
            blobClient = storageAccount.CreateCloudBlobClient();
        }

        public async Task<BlobContainer> GetBlobContainerAsync(string containerName)
        {
            if(string.IsNullOrWhiteSpace(containerName))
            {
                throw new ArgumentNullException(nameof(containerName));
            }

            var cloudBlobContainer = blobClient.GetContainerReference(containerName);
            var blobContainer = new BlobContainer(cloudBlobContainer);
            await blobContainer.SetPropertiesAsync();
            return blobContainer;
        }
    }
}