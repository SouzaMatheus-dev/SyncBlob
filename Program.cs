using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Dapper;
using System.Data.SqlClient;

internal class Program
{
    private static readonly string blobConnectionString = "";
    private static readonly string connectionString = "";
    private static readonly string containerName = "";

    private static async Task<List<ImageRecord>> GetImageRecordsAsync()
    {
        using (var connection = new SqlConnection(connectionString))
        {
            string query = "SELECT [Id], [Url] FROM [domain].[Images] WHERE EntityType = 'Profile' AND [Name] LIKE 'profile'";
            return (await connection.QueryAsync<ImageRecord>(query)).ToList();
        }
    }

    private static async Task Main(string[] args)
    {
        List<ImageRecord> imageRecords = await GetImageRecordsAsync();

        foreach (var record in imageRecords)
        {
            string newUrl = await ProcessImageAsync(record.Url);
            await UpdateImageRecordAsync(record.Id, newUrl);
        }

        Console.WriteLine("Processo concluído com sucesso.");
    }

    private static async Task<string> ProcessImageAsync(string url)
    {
        BlobServiceClient blobServiceClient = new BlobServiceClient(blobConnectionString);
        BlobContainerClient containerClient = blobServiceClient.GetBlobContainerClient(containerName);
        string blobName = url.Replace("", "");
        BlobClient blobClient = containerClient.GetBlobClient(blobName);

        if (!await blobClient.ExistsAsync())
        {
            Console.WriteLine($"Blob não encontrado: {blobName}");
            return null;
        }

        string extension = ".jpg";

        string newBlobName = blobName + extension;
        BlobClient newBlobClient = containerClient.GetBlobClient(newBlobName);

        await newBlobClient.StartCopyFromUriAsync(blobClient.Uri);

        BlobProperties properties;
        do
        {
            await Task.Delay(500);
            properties = await newBlobClient.GetPropertiesAsync();
        }
        while (properties.CopyStatus == CopyStatus.Pending);

        if (properties.CopyStatus != CopyStatus.Success)
        {
            Console.WriteLine($"Erro ao copiar o blob: {properties.CopyStatus}");
            return null;
        }

        await blobClient.DeleteAsync();

        return newBlobClient.Uri.ToString();
    }

    private static async Task UpdateImageRecordAsync(Guid id, string newUrl)
    {
        if (newUrl == null)
        {
            return;
        }

        using (var connection = new SqlConnection(connectionString))
        {
            string query = "UPDATE [domain].[Images] SET [Url] = @Url, [Name] = 'profile.jpg' WHERE [Id] = @Id";
            await connection.ExecuteAsync(query, new { Url = newUrl, Id = id });
        }
    }

    private class ImageRecord
    {
        public Guid Id { get; set; }
        public string Url { get; set; }
    }
}