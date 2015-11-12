using EasyAzureKeyVault;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.IO;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public class AzureStorage
    {
        #region Members

        private string connectionString;
        private AzureKeyVault keyVault;
        private string encryptionKeyUri;
        private string clientId;
        private string clientSecret;

        #endregion Members

        #region Properties

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

        #endregion Properties

        #region Constructors

        /// <summary>
        /// Constructs and initializes an object to download/upload/delete blob objects from Azure Storage.
        /// </summary>
        /// <param name="connectionString">Connection string to the storage account</param>
        public AzureStorage(string connectionString)
        {
            if (String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException("connectionString");
            }

            this.connectionString = connectionString;
        }

        /// <summary>
        /// Constructs and initializes an object to download/upload/delete encrypted blob objects from Azure Storage.
        /// </summary>
        /// <param name="connectionString">Connection string to the storage account</param>
        /// <param name="encryptionKeyUri">URI to the key stored in Azure Key Vault that will be used for encryption</param>
        /// <param name="clientId">Azure ActiveDirectory Client ID used to authenticate the app</param>
        /// <param name="clientSecret">Azure ActiveDirectory Client Secret used to authenticate the app</param>
        public AzureStorage(string connectionString, string encryptionKeyUri, string clientId, string clientSecret)
            : this(connectionString)
        {
            if (String.IsNullOrEmpty(encryptionKeyUri))
            {
                throw new ArgumentNullException("encryptionKeyUri");
            }

            if (String.IsNullOrEmpty(clientId))
            {
                throw new ArgumentNullException("clientId");
            }

            if (String.IsNullOrEmpty(clientSecret))
            {
                throw new ArgumentNullException("clientSecret");
            }

            this.encryptionKeyUri = encryptionKeyUri;
            this.clientId = clientId;
            this.clientSecret = clientSecret;
        }

        #endregion Constructors

        private CloudBlobContainer GetContainer(string containerName)
        {
            if (String.IsNullOrEmpty(containerName))
            {
                throw new ArgumentNullException("containerName");
            }

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
            CloudBlobContainer container = blobClient.GetContainerReference(containerName);
            return container;
        }

        #region Blob Operations

        /// <summary>
        /// Downloads and returns the contents of a blob from the specified container.
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <returns></returns>
        public async Task<byte[]> DownloadBlobAsync(string containerName, string blobName)
        {
            if (String.IsNullOrEmpty(containerName))
            {
                throw new ArgumentNullException("containerName");
            }

            if (String.IsNullOrEmpty(blobName))
            {
                throw new ArgumentNullException("blobName");
            }

            var container = GetContainer(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            using (MemoryStream ms = new MemoryStream())
            {
                await blob.DownloadToStreamAsync(ms);
                return ms.ToArray();
            }
        }

        /// <summary>
        /// Uploads a blob of a specific content type to the specified container.
        /// </summary>
        /// <param name="containerName"></param>
        /// <param name="blobName"></param>
        /// <param name="contentType"></param>
        /// <param name="blob"></param>
        /// <returns></returns>
        public async Task UploadBlobAsync(string containerName, string blobName, string contentType, byte[] blob)
        {
            if (String.IsNullOrEmpty(containerName))
            {
                throw new ArgumentNullException("containerName");
            }

            if (String.IsNullOrEmpty(blobName))
            {
                throw new ArgumentNullException("blobName");
            }

            if (String.IsNullOrEmpty(contentType))
            {
                throw new ArgumentNullException("contentType");
            }

            if (blob == null)
            {
                throw new ArgumentNullException("blob");
            }

            if (blob.Length == 0)
            {
                throw new InvalidOperationException("blob cannot be empty.");
            }

            var container = GetContainer(containerName);
            var blobRef = container.GetBlockBlobReference(blobName);
            blobRef.Properties.ContentType = contentType;
            await blobRef.UploadFromByteArrayAsync(blob, 0, blob.Length);
        }

        /// <summary>
        /// Deletes a blob from the specified container.
        /// </summary>
        /// <param name="containerName">The container to delete the blob from.</param>
        /// <param name="blobName">The name of the blob to delete.</param>
        /// <returns></returns>
        public async Task DeleteBlobAsync(string containerName, string blobName)
        {
            if (String.IsNullOrEmpty(containerName))
            {
                throw new ArgumentNullException("containerName");
            }

            if (String.IsNullOrEmpty(blobName))
            {
                throw new ArgumentNullException("blobName");
            }

            var container = GetContainer(containerName);

            var blob = container.GetBlockBlobReference(blobName);

            await blob.DeleteAsync();
        }

        #endregion Blob Operations

        #region Encrypted Blob Operations

        /// <summary>
        /// Returns the contents of an encrypted blob.
        /// </summary>
        /// <param name="containerName">Container to get the blob from</param>
        /// <param name="blobName">Name of the blob to retrieve</param>
        /// <returns></returns>
        public async Task<byte[]> DownloadEncryptedBlobAsync(string containerName, string blobName)
        {
            CheckRequiredBlobDownloadData(containerName, blobName);

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

        public byte[] DownloadEncryptedBlob(string containerName, string blobName)
        {
            CheckRequiredBlobDownloadData(containerName, blobName);

            var container = GetContainer(containerName);
            var blob = container.GetBlockBlobReference(blobName);

            BlobEncryptionPolicy policy = new BlobEncryptionPolicy(null, keyVault.KeyResolver);
            BlobRequestOptions options = new BlobRequestOptions() { EncryptionPolicy = policy };

            using (MemoryStream ms = new MemoryStream())
            {
                blob.DownloadToStream(ms, null, options, null);

                return ms.ToArray();
            }
        }

        private static void CheckRequiredBlobDownloadData(string containerName, string blobName)
        {
            if (String.IsNullOrEmpty(containerName))
            {
                throw new ArgumentNullException("containerName");
            }

            if (String.IsNullOrEmpty(blobName))
            {
                throw new ArgumentNullException("blobName");
            }
        }

        /// <summary>
        /// Encrypts and uploads a blob.
        /// </summary>
        /// <param name="containerName">Container to upload the blob to</param>
        /// <param name="blobName">Name of the blob to upload</param>
        /// <param name="contentType">MIME type of the blob to upload</param>
        /// <param name="blob">Contents of the blob to upload</param>
        /// <returns></returns>
        public async Task UploadEncryptedBlobAsync(string containerName, string blobName, string contentType, byte[] blob)
        {
            CheckRequiredBlobUploadData(containerName, blobName, contentType, blob);

            // get the container and the blob we want to upload
            var container = GetContainer(containerName);
            var blobRef = container.GetBlockBlobReference(blobName);
            blobRef.Properties.ContentType = contentType;

            // setup the encryption properties based on our encryption key in Azure Key Vault
            var encryptionKey = await keyVault.GetKeyAsync(encryptionKeyUri);
            BlobEncryptionPolicy encryptionPolicy = new BlobEncryptionPolicy(encryptionKey, null);
            BlobRequestOptions requestOptions = new BlobRequestOptions() { EncryptionPolicy = encryptionPolicy };

            await blobRef.UploadFromByteArrayAsync(blob, 0, blob.Length, null, requestOptions, null);
        }

        private static void CheckRequiredBlobUploadData(string containerName, string blobName, string contentType, byte[] blob)
        {
            if (String.IsNullOrEmpty(containerName))
            {
                throw new ArgumentNullException("containerName");
            }

            if (String.IsNullOrEmpty(blobName))
            {
                throw new ArgumentNullException("blobName");
            }

            if (String.IsNullOrEmpty(contentType))
            {
                throw new ArgumentNullException("contentType");
            }

            if (blob == null)
            {
                throw new ArgumentNullException("blob");
            }

            if (blob.Length == 0)
            {
                throw new InvalidOperationException("blob cannot be empty.");
            }
        }

        #endregion Encrypted Blob Operations
    }
}