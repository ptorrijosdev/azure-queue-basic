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
            await RunProducer();

            await RunConsumer("Consumidor 1");
            await RunConsumer("Consumidor 2");
            Console.ReadKey();
        }

        private static async Task<int> RunProducer()
        {
            var queue = GetQueue();
            int i = 1;

            var task = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    string text = string.Format("Se ha producido el mensaje {0}", i);

                    CloudQueueMessage message = new CloudQueueMessage(text);
                    queue.AddMessage(message);

                    lock (Console.Out)
                    {
                        Console.ForegroundColor = ConsoleColor.Cyan;
                        Console.WriteLine(text);
                        Console.ResetColor();
                    }

                    i++;

                    System.Threading.Thread.Sleep(500);
                }
            });

            return 1;
        }

        private static async Task<int> RunConsumer(string consumerName)
        {
            Console.WriteLine("Consumidor {0} arrancando...", consumerName);

            var queue = GetQueue();
            int i = 1;

            var task = Task.Factory.StartNew(() =>
            {
                while (true)
                {
                    var message = queue.GetMessage();

                    if (message != null)
                    {
                        string text = string.Format("El consumidor {0} ha consumido el mensaje {1}", consumerName, message.AsString);

                        lock (Console.Out)
                        {
                            Console.ForegroundColor = ConsoleColor.Green;
                            Console.WriteLine(text);
                            Console.ResetColor();
                        }
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
            string sasKey = "zTA+jRkVejMzJM1xVtPy+iC5c0Ac2Mu2e4enhGXNLTcbgj9e8Jxz67lLbey9iImoBgCt/O0uBvHO1p+khOlNuQ==";
            StorageCredentials credentials = new StorageCredentials("queuetorrijos", sasKey);

            CloudStorageAccount account = new CloudStorageAccount(credentials, true);

            var queueClient = account.CreateCloudQueueClient();

            CloudQueue queue = queueClient.GetQueueReference("torrijos-queue");

            queue.CreateIfNotExists();

            return queue;
        }
    }
}
