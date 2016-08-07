using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using Newtonsoft.Json;
using System;

namespace EasyAzureStorage
{
    public class AzureStorageQueue
    {
        private string connectionString;
        private CloudQueue queue;

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
            queueName = queueName.Trim().ToLower();
            this.SetupQueue(queueName);
        }

        /// <summary>
        /// Gets a reference to the queue in our currently connected storage account. The queue is created
        /// if it doesn't exist.
        /// </summary>
        /// <param name="queueName"></param>
        private void SetupQueue(string queueName)
        {
            if (String.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException("queueName");
            }

            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString);
            CloudQueueClient queueClient = storageAccount.CreateCloudQueueClient();

            this.queue = queueClient.GetQueueReference(queueName);
            this.queue.CreateIfNotExists();
        }

        /// <summary>
        /// Sends a message to the queue.
        /// </summary>
        /// <param name="message"></param>
        public void SendMessage(string message)
        {
            if (String.IsNullOrEmpty(message))
            {
                throw new ArgumentNullException("queueName");
            }

            CloudQueueMessage newMessage = new CloudQueueMessage(message);
            queue.AddMessage(newMessage);
        }

        /// <summary>
        /// Sends a message to the queue.
        /// </summary>
        /// <param name="message"></param>
        public void SendMessage(byte[] message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            CloudQueueMessage newMessage = new CloudQueueMessage(message);
            queue.AddMessage(newMessage);
        }

        /// <summary>
        /// Sends a message of a generic type to the queue.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="message"></param>
        public void SendMessage<T>(T message)
        {
            if (message == null)
            {
                throw new ArgumentNullException("message");
            }

            string json = JsonConvert.SerializeObject(message);
            SendMessage(json);
        }

        /// <summary>
        /// Peeks at the first message on the queue and returns as a string.
        /// </summary>
        /// <returns></returns>
        public string PeekMessage()
        {
            CloudQueueMessage peekedMessage = queue.PeekMessage();
            return peekedMessage.AsString;
        }

        /// <summary>
        /// Peeks at the first message on the queue and returns as a byte array.
        /// </summary>
        /// <returns></returns>
        public byte[] PeekMessageBytes()
        {
            CloudQueueMessage peekedMessage = queue.PeekMessage();
            return peekedMessage.AsBytes;
        }

        /// <summary>
        /// Peeks at the first message on the queue and returns as a generic type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T PeekMessageObject<T>()
        {
            string json = PeekMessage();
            return JsonConvert.DeserializeObject<T>(json);
        }

        /// <summary>
        /// Gets and dequeues the first message on the queue and returns as a string.
        /// </summary>
        /// <returns></returns>
        public string GetMessage()
        {
            CloudQueueMessage retrievedMessage = queue.GetMessage();
            queue.DeleteMessage(retrievedMessage);
            return retrievedMessage.AsString;
        }

        /// <summary>
        /// Gets and dequeues the first message on the queue and returns as a byte array.
        /// </summary>
        /// <returns></returns>
        public byte[] GetMessageBytes()
        {
            CloudQueueMessage retrievedMessage = queue.GetMessage();
            queue.DeleteMessage(retrievedMessage);
            return retrievedMessage.AsBytes;
        }

        /// <summary>
        /// Gets the first message on the queue and returns as a byte array.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public T GetMessageObject<T>()
        {
            string json = GetMessage();
            return JsonConvert.DeserializeObject<T>(json);
        }
    }
}