using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;
using System.Threading.Tasks;

namespace EasyAzureStorage
{
    public class AzureStorageQueue
    {
        private string queueName;
        private string connectionString;

        private CloudQueue queue;

        private async Task<CloudQueue> SetupQueueAsync()
        {
            if (queue == null)
            {
                CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
                CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();
                await this.queue.CreateIfNotExistsAsync();
                this.queue = queueClient.GetQueueReference(queueName);
            }

            return queue;
        }

        /// <summary>
        /// Constructs and initializes an object to download/upload/delete blob objects from Azure Storage.
        /// </summary>
        /// <param name="connectionString">Connection string to the storage account</param>
        public AzureStorageQueue(string connectionString, string queueName)
        {
            if (String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException("connectionString");
            }

            if (String.IsNullOrEmpty(queueName))
            {
                throw new ArgumentNullException("queueName");
            }

            this.connectionString = connectionString;
            this.queueName = queueName.Trim().ToLower();
        }
        
        /// <summary>
        /// Sends a message to the queue.
        /// </summary>
        /// <param name="message"></param>
        public async Task SendMessageAsync(string message)
        {
            if (String.IsNullOrEmpty(message))
            {
                throw new ArgumentNullException("queueName");
            }

            await SetupQueueAsync();

            CloudQueueMessage newMessage = new CloudQueueMessage(message);
            await queue.AddMessageAsync(newMessage);
        }

        /// <summary>
        /// Sends a message to the queue.
        /// </summary>
        /// <param name="message"></param>
        public async Task SendMessageAsync(byte[] message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            await SetupQueueAsync();

            CloudQueueMessage newMessage = CloudQueueMessage.CreateCloudQueueMessageFromByteArray(message);
            await queue.AddMessageAsync(newMessage);
        }

        /// <summary>
        /// Sends a message of a generic type to the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message"></param>
        public async Task SendMessageAsync<T>(T message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            await SetupQueueAsync();

            string json = JsonConvert.SerializeObject(message);
            await SendMessageAsync(json);
        }

        /// <summary>
        /// Peeks at the first message on the queue and returns as a string.
        /// </summary>
        /// <returns></returns>
        public async Task<string> PeekMessageAsync()
        {
            await SetupQueueAsync();

            CloudQueueMessage peekedMessage = await queue.PeekMessageAsync();
            return peekedMessage.AsString;
        }

        /// <summary>
        /// Peeks at the first message on the queue and returns as a byte array.
        /// </summary>
        /// <returns></returns>
        public async Task<byte[]> PeekMessageBytesAsync()
        {
            await SetupQueueAsync();

            CloudQueueMessage peekedMessage = await queue.PeekMessageAsync();
            return peekedMessage.AsBytes;
        }

        /// <summary>
        /// Peeks at the first message on the queue and returns as a generic type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public async Task<T> PeekMessageObject<T>()
        {
            await SetupQueueAsync();
            string json = await PeekMessageAsync();
            return JsonConvert.DeserializeObject<T>(json);
        }

        /// <summary>
        /// Gets and dequeues the first message on the queue and returns as a string.
        /// </summary>
        /// <returns></returns>
        public async Task<string> GetMessageAsync()
        {
            await SetupQueueAsync();
            CloudQueueMessage retrievedMessage = await queue.GetMessageAsync();
            await queue.DeleteMessageAsync(retrievedMessage);
            return retrievedMessage.AsString;
        }

        /// <summary>
        /// Gets and dequeues the first message on the queue and returns as a byte array.
        /// </summary>
        /// <returns></returns>
        public async Task<byte[]> GetMessageBytesAsync()
        {
            await SetupQueueAsync();
            CloudQueueMessage retrievedMessage = await queue.GetMessageAsync();
            await queue.DeleteMessageAsync(retrievedMessage);
            return retrievedMessage.AsBytes;
        }

        /// <summary>
        /// Gets the first message on the queue and returns as a byte array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public async Task<T> GetMessageObjectAsync<T>()
        {
            await SetupQueueAsync();
            string json = await GetMessageAsync();
            return JsonConvert.DeserializeObject<T>(json);
        }
    }
}