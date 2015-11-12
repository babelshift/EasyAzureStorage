using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public class AzureStorage : BaseAzureStorage
    {
        public AzureStorage(string connectionString) : base(connectionString) { }

        public async Task<byte[]> DownloadBlobAsync(string containerName, string blobName)
        {
            var container = GetContainer(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            
            using (MemoryStream ms = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(ms);
                return ms.ToArray();
            }
        }

        public async Task UploadBlobAsync(string containerName, string blobName, string contentType, byte[] blob)
        {
            var container = GetContainer(containerName);
            var blobRef = container.GetBlockBlobReference(blobName);
            blobRef.Properties.ContentType = contentType;
            await blobRef.UploadFromByteArrayAsync(blob, 0, blob.Length);
        }
    }
}
