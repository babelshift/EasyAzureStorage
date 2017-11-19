using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public class Blob
    {
        private ICloudBlob blob;
        private BlobType blobType;
        private byte[] contents;

        public BlobProperties Properties
        {
            get { return blob.Properties; }
        }

        public byte[] AsByteArray
        {
            get { return contents; }
        }

        public string AsString
        {
            get { return System.Text.Encoding.UTF8.GetString(contents); }
        }

        internal Blob(ICloudBlob blob, BlobType blobType)
        {
            this.blob = blob ?? throw new ArgumentNullException(nameof(blob));
            this.blobType = blobType;
        }

        internal async Task SetContentsAsync()
        {
            using (MemoryStream ms = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(ms);
                contents = ms.ToArray();
            }
        }

        internal async Task SetPropertiesAsync()
        {
            await blob.FetchAttributesAsync();
        }
    }
}