using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Helpers;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;
using ElasticsearchExamples;

const string IndexName = "stock-demo-v1";

var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
    .CertificateFingerprint("E8:76:3D:91:81:8C:57:31:6F:2F:E0:4C:17:78:78:FB:38:CC:37:27:41:7A:94:B4:12:AA:B6:D1:D6:C4:4C:7D")
    .Authentication(new BasicAuthentication("elastic", "password"));

var client = new ElasticsearchClient(settings);

var existsResponse = await client.Indices.ExistsAsync(IndexName);

if (!existsResponse.Exists)
{
    var newIndexResponse = await client.Indices.CreateAsync(IndexName, i => i
        .Mappings(m => m
            .Properties(new Elastic.Clients.Elasticsearch.Mapping.Properties
            {
                { "symbol", new KeywordProperty() },
                { "high", new FloatNumberProperty() },
                { "low", new FloatNumberProperty() },
                { "open", new FloatNumberProperty() },
                { "close", new FloatNumberProperty() },
            }))
        //.Map(m => m
        //    .AutoMap<StockData>()
        //    .Properties<StockData>(p => p.Keyword(k => k.Name(n => n.Symbol))))
        .Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));

    if (!newIndexResponse.IsValid || newIndexResponse.Acknowledged is false) throw new Exception("Oh no!");

    try
    {
        var bulkAll = client.BulkAll(ReadStockData(), r => r
           .Index(IndexName)
           .BackOffRetries(20)
           .BackOffTime(TimeSpan.FromSeconds(10))
           .ContinueAfterDroppedDocuments()
           .DroppedDocumentCallback((r, d) =>
           {
               Console.WriteLine(r.Error.Reason);
           })
           .MaxDegreeOfParallelism(4)
           .Size(1000));

        bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));
    }
    catch (Exception ex)
    {
        Console.WriteLine(ex.ToString());
    }
}

static IEnumerable<StockData> ReadStockData()
{
    var file = new StreamReader("c:\\stock-data\\all_stocks_5yr.csv");

    string line;
    while ((line = file.ReadLine()) is not null)
    {
        yield return new StockData(line);
    }
}

Console.WriteLine("Press any key to exit.");
Console.ReadKey();