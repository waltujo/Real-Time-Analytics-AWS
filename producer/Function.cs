using Amazon.Kinesis;
using Amazon.Kinesis.Model;
using Amazon.Lambda.Core;
using Newtonsoft.Json;
using System.Text;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace producer;

public class Function
{
    private static readonly double latitude = -29.6846;
    private static readonly double longitude = -51.1419;
    private static readonly string TOMORROW_API_KEY = Environment.GetEnvironmentVariable("TOMORROW_API_KEY");
    private static readonly string url = $"https://api.tomorrow.io/v4/weather/realtime?location={latitude},{longitude}&apikey={TOMORROW_API_KEY}";
    private static readonly HttpClient client = new HttpClient { DefaultRequestHeaders = { { "accept", "application/json" } } };
    private static readonly string STREAM_NAME = "broker";
    private static readonly string REGION = "us-east-1";
    private static readonly AmazonKinesisClient kinesisClient = new AmazonKinesisClient(Amazon.RegionEndpoint.GetBySystemName(REGION));


    /// <summary>
    /// A simple function that makes a data to Kinesis
    /// </summary>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    public async Task<object> FunctionHandler(object input, ILambdaContext context)
    {
        try
        {
            // Faz a requisição à API
            HttpResponseMessage response = await client.GetAsync(url);
            response.EnsureSuccessStatusCode();
            var weatherData = await response.Content.ReadAsStringAsync();

            // Envia os dados para o Kinesis
            await SendToKinesis(weatherData);

            context.Logger.LogLine("Dados enviados ao Kinesis com sucesso.");

            return new
            {
                statusCode = 200,
                body = JsonConvert.SerializeObject("Dados enviados ao Kinesis com sucesso")
            };
        }
        catch (Exception ex)
        {
            context.Logger.LogLine($"Erro: {ex.Message}");
            throw;
        }
    }

    private async Task SendToKinesis(string data)
    {
        using (var kinesisClient = new AmazonKinesisClient(Amazon.RegionEndpoint.USEast1))
        {
            var request = new PutRecordRequest
            {
                StreamName = STREAM_NAME,
                Data = new MemoryStream(Encoding.UTF8.GetBytes(data)),
                PartitionKey = "1"
            };

            var response = await kinesisClient.PutRecordAsync(request);

            if (response.HttpStatusCode != System.Net.HttpStatusCode.OK)
            {
                throw new Exception("Falha ao enviar dados para o Kinesis.");
            }
        }
    }
}
