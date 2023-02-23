using Confluent.Kafka;
using System.Text;
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;

namespace IoT_Service_With_Distrubuted_Architecture.IoTService
{
    //First i create a background service and i make it wotk every 10 second 
    public class MqttToKafkaService : BackgroundService
    {
        private readonly PeriodicTimer _timer = new(TimeSpan.FromSeconds(10));
        public static string mac_address;

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (await _timer.WaitForNextTickAsync(stoppingToken)
            && !stoppingToken.IsCancellationRequested)
            {
                await DoWorkAsync();
            }
        }
        static async void client_MqttMsgPublishReceived(
        object sender, MqttMsgPublishEventArgs e)
        {
            //I take the data from topic I subscribe in 57. row and out it into message variable
            var message = Encoding.Default.GetString(e.Message);
            //I connect to kafka servce here
            var config = new ProducerConfig { BootstrapServers = "Your Kafka Server and Port:{For Example :163.18.44.75:9092}" };

            using (var p = new ProducerBuilder<Null, string>(config).Build())
            {
                try
                {
                    //I Produce IoT data to kafka topic that name IoTData
                    var dr = await p.ProduceAsync("IoTData", new Message<Null, string> { Value = message });

                }
                catch (ProduceException<Null, string> ex)
                {
                    Console.WriteLine($"Delivery failed: {ex.Error.Reason}");
                }
            }
            //I execute garbage collecter here if you dont it will not excute himself and use tons of ram
            GC.Collect();
        }
        //this code will work every 10 second
        public static async Task DoWorkAsync()
        {
            //I Define my mqtt client here
            MqttClient client;
            client = new MqttClient("You mqtt client(For Example:192.16.64.11)");
            client.MqttMsgPublishReceived += client_MqttMsgPublishReceived;
            client.Connect("You mqtt client(For Example:192.16.64.11)");
            //Then I Publish get_data to the topic that make devices publish IoT Data to their topic  
            client.Publish("/data/db/topic", Encoding.UTF8.GetBytes("get_data"), MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE, true);
            //Then I subscribe to the topic that give me IoT data from devices every time i publish to publish topic that in the 55. row code
            client.Subscribe(new string[] { "/data/db/response/topic/#" }, new byte[] { MqttMsgBase.QOS_LEVEL_EXACTLY_ONCE });
            client.Disconnect();
        }
    }
}
