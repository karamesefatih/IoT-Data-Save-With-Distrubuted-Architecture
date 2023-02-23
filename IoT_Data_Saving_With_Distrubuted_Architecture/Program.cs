using IoT_Service_With_Distrubuted_Architecture.IoTService;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddHostedService<KafkaToMangoDbService>();
builder.Services.AddHostedService<MqttToKafkaService>();

var app = builder.Build();



app.Run();
