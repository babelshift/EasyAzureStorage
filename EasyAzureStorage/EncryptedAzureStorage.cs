using EasyAzureKeyVault;
using Microsoft.WindowsAzure.Storage.Blob;
using System.IO;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    /// <summary>
    /// Represents an Azure Storage object which has its contents encrypted and decrypted based on an encryption key
    /// stored in Azure Key Vault. Requires an Azure subscription, and Azure storage container, and an Azure KeyVault key.
    /// </summary>
    public class EncryptedAzureStorage : BaseAzureStorage
    {
        private AzureKeyVault keyVault;
        private string encryptionKeyUri;
        private string clientId;
        private string clientSecret;

        private AzureKeyVault KeyVault
        {
            get
            {
                if (keyVault == null)
                {
                    keyVault = new AzureKeyVault(clientId, clientSecret);
                }

                return keyVault;
            }
        }

        /// <summary>
        /// Constructs and initialize an object to download/upload/delete encrypted blob objects from Azure Storage.
        /// </summary>
        /// <param name="connectionString">Connection string to the storage account</param>
        /// <param name="encryptionKeyUri">URI to the key stored in Azure Key Vault that will be used for encryption</param>
        /// <param name="clientId">Azure ActiveDirectory Client ID used to authenticate the app</param>
        /// <param name="clientSecret">Azure ActiveDirectory Client Secret used to authenticate the app</param>
        public EncryptedAzureStorage(string connectionString, string encryptionKeyUri, string clientId, string clientSecret)
            : base(connectionString)
        {
            this.encryptionKeyUri = encryptionKeyUri;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }

        /// <summary>
        /// Returns the contents of an encrypted blob.
        /// </summary>
        /// <param name="containerName">Container to get the blob from</param>
        /// <param name="blobName">Name of the blob to retrieve</param>
        /// <returns></returns>
        public async Task<byte[]> GetEncryptedBlobContentsAsync(string containerName, string blobName)
        {
            var container = GetContainer(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            BlobEncryptionPolicy policy = new BlobEncryptionPolicy(null, keyVault.KeyResolver);
            BlobRequestOptions options = new BlobRequestOptions() { EncryptionPolicy = policy };

            using (MemoryStream ms = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(ms, null, options, null);

                return ms.ToArray();
            }
        }

        /// <summary>
        /// Encrypts and uploads a blob.
        /// </summary>
        /// <param name="containerName">Container to upload the blob to</param>
        /// <param name="blobName">Name of the blob to upload</param>
        /// <param name="contentType">MIME type of the blob to upload</param>
        /// <param name="blobContents">Contents of the blob to upload</param>
        /// <returns></returns>
        public async Task UploadEncryptedBlobAsync(string containerName, string blobName, string contentType, byte[] blobContents)
        {
            // get the container and the blob we want to upload
            var container = GetContainer(containerName);
            var blob = container.GetBlockBlobReference(blobName);
            blob.Properties.ContentType = contentType;

            // setup the encryption properties based on our encryption key in Azure Key Vault
            var encryptionKey = await keyVault.GetKeyAsync(encryptionKeyUri);
            BlobEncryptionPolicy encryptionPolicy = new BlobEncryptionPolicy(encryptionKey, null);
            BlobRequestOptions requestOptions = new BlobRequestOptions() { EncryptionPolicy = encryptionPolicy };

            await blob.UploadFromByteArrayAsync(blobContents, 0, blobContents.Length, null, requestOptions, null);
        }
    }
}