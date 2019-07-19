using System;
using System.Threading.Tasks;
using Microsoft.Azure;
using Microsoft.Azure.Storage;
using Microsoft.Azure.Storage.Queue;
using Microsoft.Azure.Storage.Auth;

namespace Azure.Queue.Basic
{
    class Program
    {
        static void Main(string[] args)
        {
            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            // Run main tasks
            await RunProducer();
            await RunConsumer("First consumer");
            await RunConsumer("Second consumer");

            // Wait program, do not close yet
            Console.ReadKey();
        }

        private static async Task<int> RunProducer()
        {
            Console.WriteLine("Producer starting...");

            // Get the Azure Storage Queue Client
            var queue = GetQueue();
            int i = 1;

            var task = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    // Create a new message
                    string text = string.Format("One message has been stored: {0}", i);
                    CloudQueueMessage message = new CloudQueueMessage(text);

                    // Enqueue the new message
                    queue.AddMessage(message);

                    lock (Console.Out)
                    {
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine(text);
                        Console.ResetColor();
                    }

                    i++;

                    // Wait half a second, so we can read the trace on console. Thanks, you Billy the Kid CPU
                    System.Threading.Thread.Sleep(500);
                }
            });

            return 1;
        }

        private static async Task<int> RunConsumer(string consumerName)
        {
            Console.WriteLine("Consumer \"{0}\" starting...", consumerName);

            // Get the Azure Storage Queue Client
            var queue = GetQueue();
            int i = 1;

            var task = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    // Dequeue a new message from our Azure Storage Queue
                    var message = queue.GetMessage();

                    if (message != null)
                    {
                        string text = string.Format("The consumer \"{0}\" has retrieved a message: {1}", consumerName, message.AsString);

                        lock (Console.Out)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine(text);
                            Console.ResetColor();
                        }
                        // Remove the message from the queue
                        queue.DeleteMessage(message);

                        i++;
                    }
                    System.Threading.Thread.Sleep(500);
                }
            });

            return 1;
        }

        private static CloudQueue GetQueue()
        {
            string sasKey = "SECRET_KEY";
            StorageCredentials credentials = new StorageCredentials("queuetorrijos", sasKey);

            CloudStorageAccount account = new CloudStorageAccount(credentials, true);

            var queueClient = account.CreateCloudQueueClient();

            CloudQueue queue = queueClient.GetQueueReference("torrijos-queue");

            queue.CreateIfNotExists();

            return queue;
        }
    }
}
