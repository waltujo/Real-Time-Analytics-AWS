using System.Text;
using Amazon.Lambda.Core;
using Amazon.Lambda.KinesisEvents;
using Amazon.S3;
using Amazon.S3.Model;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace consumer_batch;

public class Function
{
    private static readonly string BUCKET_NAME = Environment.GetEnvironmentVariable("BUCKET_NAME");
    private static readonly IAmazonS3 s3Client = new AmazonS3Client();

    public async Task<object> FunctionHandler(KinesisEvent kinesisEvent, ILambdaContext context)
    {
        context.Logger.LogInformation($"Beginning to process {kinesisEvent.Records.Count} records...");

        foreach (var record in kinesisEvent.Records)
        {
            try
            {
                var payload = Encoding.UTF8.GetString(record.Kinesis.Data.ToArray());
                var data = JsonConvert.DeserializeObject<JObject>(payload);

                var now = DateTime.Now;
                var year = now.Year;
                var month = now.Month;
                var day = now.Day;

                var fileName = $"raw/year={year}/month={month}/day={day}/weather_data_{now.ToString("dd-MM-yyyy-HH:mm:ss")}.json";

                // Serializa os dados JSON e envia para o S3
                await UploadToS3Async(data, fileName, context);
            }
            catch (Exception ex)
            {
                context.Logger.LogLine($"Erro geral: {ex.Message}");
            }
        }

        context.Logger.LogInformation("Stream processing complete.");

        return new
        {
            statusCode = 200,
            body = JsonConvert.SerializeObject("Dados salvos no S3 com sucesso")
        };
    }

    private async Task UploadToS3Async(object data, string fileName, ILambdaContext context)
    {
        try
        {
            var jsonData = JsonConvert.SerializeObject(data);

            var putRequest = new PutObjectRequest
            {
                BucketName = BUCKET_NAME,
                Key = fileName,
                ContentBody = jsonData,
                ContentType = "application/json"
            };

            var response = await s3Client.PutObjectAsync(putRequest);

            if (response.HttpStatusCode == System.Net.HttpStatusCode.OK)
            {
                context.Logger.LogLine("Dados salvos no S3 com sucesso!");
            }
            else
            {
                context.Logger.LogLine("Falha ao salvar os dados no S3.");
            }

            context.Logger.LogLine($"Dados salvos no S3 com sucesso. S3 Response: {response.HttpStatusCode}");
        }
        catch (Exception ex)
        {
            context.Logger.LogLine($"Erro ao enviar dados para o S3: {ex.Message}");
            throw;
        }
    }
}