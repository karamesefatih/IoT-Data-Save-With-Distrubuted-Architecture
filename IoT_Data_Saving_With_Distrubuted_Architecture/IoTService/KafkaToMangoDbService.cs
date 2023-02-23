using Confluent.Kafka;
using MongoDB.Bson;
using MongoDB.Driver;

namespace IoT_Service_With_Distrubuted_Architecture.IoTService
{
    public class KafkaToMangoDbService : BackgroundService
    {
        //Work prensibe is the same with Mqtt To Kafka service
        //But when program connect to kafka service it will go to endless loop so the time we give doesn't matter 
        public static string mac_address;

        private readonly PeriodicTimer _timer = new(TimeSpan.FromSeconds(10));
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {

            while (await _timer.WaitForNextTickAsync(stoppingToken)
            && !stoppingToken.IsCancellationRequested)
            {
                await DoWorkAsync();
            }


        }
        public static async Task DoWorkAsync()
        {
            //Connecting to mongo db server here
            var client = new MongoClient("Your mongo db connection string here");
            var database = client.GetDatabase("IoTDatabase");
            var collec = database.GetCollection<BsonDocument>("IoTCollection");
            //Connecting to kafka server here
            var config = new ConsumerConfig
            {
                BootstrapServers = "Your kafka server here",
                GroupId = "IoT",
                AutoOffsetReset = AutoOffsetReset.Earliest,
            };
            using (var c = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                //Consuming IoTData topic here
                c.Subscribe("IoTData");
                CancellationTokenSource cts = new CancellationTokenSource();
                Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true; // prevent the process from terminating.
                    cts.Cancel();
                };
                try
                {
                    //Once we are in th kafka topic we are in the endless loop here unless something wen wrong
                    while (true)
                    {
                        //Taking the data from topic and putting to vr variable
                        var cr = c.Consume(cts.Token);
                        //Sending the IoT Data to mongo db with time value here 
                        var IoTData = new BsonDocument{
                            { BsonDocument.Parse(cr.Value) },
                            { "Time",DateTime.UtcNow.AddHours(3) }
                        };
                        await collec.InsertOneAsync(IoTData);

                    }
                }
                catch (OperationCanceledException)
                {
                    c.Close();
                }
            }
        }
    }
}