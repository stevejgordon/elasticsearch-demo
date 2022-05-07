using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.Helpers;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;

namespace ElasticsearchExamples;

internal class Version8
{
    public static async Task ExecuteAsync()
    {
        const string IndexName = "stock-demo-v8";
        const string CloudId = "CLOUDID";

        //var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
        //    .CertificateFingerprint("E8:76:3D:91:81:8C:57:31:6F:2F:E0:4C:17:78:78:FB:38:CC:37:27:41:7A:94:B4:12:AA:B6:D1:D6:C4:4C:7D")
        //    .Authentication(new BasicAuthentication("elastic", "password"));

        var client = new ElasticsearchClient(CloudId, new BasicAuthentication("elastic", "password"));

        var existsResponse = await client.Indices.ExistsAsync(IndexName);

        if (!existsResponse.Exists)
        {
            var newIndexResponse = await client.Indices.CreateAsync(IndexName, i => i
                .Mappings(m => m
                    .Properties(new Properties
                    {
                        { "symbol", new KeywordProperty() },
                        { "high", new FloatNumberProperty() },
                        { "low", new FloatNumberProperty() },
                        { "open", new FloatNumberProperty() },
                        { "close", new FloatNumberProperty() },
                    }))
                .Settings(s => s.NumberOfShards(1).NumberOfReplicas(0)));

            if (!newIndexResponse.IsValid || newIndexResponse.Acknowledged is false) throw new Exception("Oh no!");

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

        var symbolResponse = await client.SearchAsync<StockData>(s => s
                .Index(IndexName)
                .Query(q => q
                    .Bool(b => b
                        .Filter(f => f.Term(t => t.Field(f => f.Symbol).Value("MSFT")))))
                .Size(20)
                .Sort(srt => srt.Descending(d => d.Date)));

        if (!symbolResponse.IsValid) throw new Exception("Oh no");

        foreach (var data in symbolResponse.Documents)
        {
            Console.WriteLine($"{data.Date:d}   {data.High:n2} {data.Low:n2}");
        }

        var fullTextSearchResponse = await client.SearchAsync<StockData>(s => s
            .Index(IndexName)
            .Query(q => q
                .Match(m => m.Field(f => f.Name).Query("inc")))
            .Size(20)
            .Sort(srt => srt.Descending(d => d.Date)));

        if (!fullTextSearchResponse.IsValid) throw new Exception("Oh no");

        foreach (var data in fullTextSearchResponse.Documents)
        {
            Console.WriteLine($"{data.Name} {data.Date:d}   {data.High:n2} {data.Low:n2}");
        }

        var aggExampleResponse = await client.SearchAsync<StockData>(s => s
            .Index(IndexName)
            .Size(0)
                .Query(q => q
                    .Bool(b => b
                        .Filter(f => f.Term(t => t.Field(f => f.Symbol).Value("MSFT")))))
            .Aggregations(a => a
                .DateHistogram("by-month", dh => dh
                    .CalendarInterval(CalendarInterval.Month)
                    .Field(fld => fld.Date)
                    .Order(HistogramOrder.KeyDescending)
                    .Aggregations(agg => agg
                        .Sum("trade-volumes", sum => sum.Field(fld => fld.Volume))))));

        if (!aggExampleResponse.IsValid) throw new Exception("Oh no");

        var monthlyBuckets = aggExampleResponse.Aggregations.GetDateHistogram("by-month").Buckets;

        foreach (var monthlyBucket in monthlyBuckets)
        {
            var volume = monthlyBucket.GetSum("trade-volumes").Value;
            Console.WriteLine($"{monthlyBucket.Key.DateTimeOffset:yyyy-MM} : {volume:n0}");
        }

        static IEnumerable<StockData> ReadStockData()
        {
            var file = new StreamReader("c:\\stock-data\\all_stocks_5yr.csv");
            file.ReadLine(); // Skip the header

            string line;
            while ((line = file.ReadLine()) is not null)
            {
                yield return StockData.ParseFromFileLine(line);
            }
        }
    }
}
