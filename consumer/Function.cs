using System.Text;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Amazon.SimpleNotificationService;
using Amazon.SimpleNotificationService.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace consumer;

public class Function
{
    private static readonly IAmazonSimpleNotificationService _snsClient = new AmazonSimpleNotificationServiceClient();

    private static readonly string SNS_TOPIC_ARN = "crie seu alerta SNS";

    private static readonly int PRECIPITATION_PROBABILITY_THRESHOLD = int.Parse(Environment.GetEnvironmentVariable("PRECIPITATION_PROBABILITY") ?? "0");
    private static readonly int WIND_SPEED_THRESHOLD = int.Parse(Environment.GetEnvironmentVariable("WIND_SPEED") ?? "0");
    private static readonly int WIND_GUST_THRESHOLD = int.Parse(Environment.GetEnvironmentVariable("WIND_GUST") ?? "0");
    private static readonly int RAIN_INTENSITY_THRESHOLD = int.Parse(Environment.GetEnvironmentVariable("RAIN_INTENSITY") ?? "0");

    public async Task<StreamsEventResponse> FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
    {
        if (kinesisEvent.Records == null || kinesisEvent.Records.Count == 0)
        {
            context.Logger.LogLine("No records found in the event.");

            return new StreamsEventResponse();
        }

        foreach (var record in kinesisEvent.Records)
        {
            try
            {
                var data = await GetRecordDataAsync(record.Kinesis, context);

                // Extrai a seção 'data' e 'values'
                if (data != null && data.TryGetValue("data", out var dataContent))
                {
                    var values = dataContent["values"] as JObject;

                    if (values != null)
                    {
                        // Extraí os valores como double
                        var precipitationProbability = values.Value<double?>("precipitationProbability") ?? 0;
                        var windSpeed = values.Value<double?>("windSpeed") ?? 0;
                        var windGust = values.Value<double?>("windGust") ?? 0;
                        var rainIntensity = values.Value<double?>("rainIntensity") ?? 0;

                        // Verifica se algum valor excede os limites configurados
                        if (precipitationProbability >= PRECIPITATION_PROBABILITY_THRESHOLD ||
                            windSpeed >= WIND_SPEED_THRESHOLD ||
                            windGust >= WIND_GUST_THRESHOLD ||
                            rainIntensity >= RAIN_INTENSITY_THRESHOLD)
                        {
                            var message = $"Probabilidade de Chuva: {precipitationProbability}%\n" +
                                          $"Velocidade do Vento: {windSpeed} m/s\n" +
                                          $"Rajada de Vento: {windGust} m/s\n" +
                                          $"Intensidade da Chuva: {rainIntensity} mm/h\n";

                            var snsRequest = new PublishRequest
                            {
                                TopicArn = SNS_TOPIC_ARN,
                                Message = message,
                                Subject = "Alerta Meteorológico"
                            };

                            var response = await _snsClient.PublishAsync(snsRequest);

                            context.Logger.LogLine($"SNS response: {response.MessageId} - SNS Status Code: {response.HttpStatusCode}");
                        }
                    }
                }
            }
            catch (JsonException ex)
            {
                context.Logger.LogLine($"Erro ao desserializar JSON: {ex.Message}");
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Erro geral: {ex.Message}");
            }
        }

        return new StreamsEventResponse();
    }

    private async Task<JObject> GetRecordDataAsync(KinesisEvent.Record record, ILambdaContext context)
    {
        // Decodificar o dado do Kinesis de Base64 para string JSON
        var payload = Encoding.UTF8.GetString(record.Data.ToArray());

        // Usa JObject para lidar com dados dinâmicos JSON
        var data = JsonConvert.DeserializeObject<JObject>(payload);

        if (data == null)
            return new JObject() 
            {
                {"MessageError", "data is empty." }
            };
      
        await Task.CompletedTask;

        return data;
    }
}