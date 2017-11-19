using Microsoft.WindowsAzure.Storage.Blob;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public class BlobContainer
    {
        private CloudBlobContainer container;
        private BlobContainerProperties properties;

        public BlobContainerProperties Properties
        {
            get { return properties; }
        }

        internal BlobContainer(CloudBlobContainer container)
        {
            this.container = container ?? throw new ArgumentNullException(nameof(container));
        }

        internal async Task SetPropertiesAsync()
        {
            await container.FetchAttributesAsync();
            properties = container.Properties;
        }

        public async Task<Blob> GetBlobAsync(string blobName, BlobType blobType)
        {
            if (String.IsNullOrWhiteSpace(blobName))
            {
                throw new ArgumentNullException(nameof(blobName));
            }

            var cloudBlobReference = GetCloudBlobReference(blobName, blobType);
            Blob blob = new Blob(cloudBlobReference, blobType);

            List<Task> tasks = new List<Task>()
            {
                blob.SetPropertiesAsync(),
                blob.SetContentsAsync()
            };

            await Task.WhenAll(tasks);

            return blob;
        }

        public async Task CreateBlobAsync(string blobName, BlobType blobType, string contentType, byte[] content)
        {
            await UploadBlobAsync(blobName, blobType, contentType, content);
        }

        public async Task CreateBlobAsync(string blobName, BlobType blobType, string contentType, string content)
        {
            byte[] binaryContent = Encoding.UTF8.GetBytes(content);
            await UploadBlobAsync(blobName, blobType, contentType, binaryContent);
        }

        public async Task CreateBlobAsync<T>(string blobName, BlobType blobType, string contentType, T content)
        {
            var blobString = JsonConvert.SerializeObject(content);
            byte[] binaryContent = Encoding.UTF8.GetBytes(blobString);
            await UploadBlobAsync(blobName, blobType, contentType, binaryContent);
        }

        private async Task UploadBlobAsync(string blobName, BlobType blobType, string contentType, byte[] content)
        {
            var blob = GetCloudBlobReference(blobName, blobType);
            blob.Properties.ContentType = contentType;
            await blob.UploadFromByteArrayAsync(content, 0, content.Length);
        }

        public async Task DeleteBlobAsync(string blobName)
        {
            var cloudBlob = GetCloudBlobReference(blobName);
            await cloudBlob.DeleteAsync();
        }

        private CloudBlob GetCloudBlobReference(string blobName)
        {
            return container.GetBlobReference(blobName);
        }

        private ICloudBlob GetCloudBlobReference(string blobName, BlobType blobType)
        {
            if (blobType == BlobType.BlockBlob)
            {
                return container.GetBlockBlobReference(blobName);
            }
            else if (blobType == BlobType.PageBlob)
            {
                return container.GetPageBlobReference(blobName);
            }
            else if (blobType == BlobType.AppendBlob)
            {
                return container.GetAppendBlobReference(blobName);
            }
            else
            {
                throw new InvalidOperationException($"{blobType} is an unsupported Blob Type.");
            }
        }
    }
}