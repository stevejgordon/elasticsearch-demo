using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Transport;

namespace ElasticsearchExamples;

internal class Version8
{
    public static async Task ExecuteAsync()
    {
        const string IndexName = "stock-demo-v8";
        const string CloudId = "CLOUDID";

        #region 'panic mode' docker fallabck

        //var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
        //    .CertificateFingerprint("03:48:48:2C:5A:F4:20:C9:28:6E:43:8C:34:37:8E:3A:C8:E6:E4:46:01:48:AD:BC:A2:2D:50:D0:2B:1E:A0:DB")
        //    .Authentication(new BasicAuthentication("elastic", "password"));
        //var client = new ElasticsearchClient(settings);

        #endregion

        var client = new ElasticsearchClient(CloudId,
            new BasicAuthentication("elastic", "password"));

        var existsResponse = await client.Indices.ExistsAsync(IndexName);

        if (!existsResponse.Exists)
        {
            var newIndexResponse = await client.Indices.CreateAsync<StockData>(IndexName, i => i
                .Mappings(m => m
                    .Properties(p => p
                        .Keyword(n => n.Symbol)
                        .FloatNumber(n => n.Open)
                        .FloatNumber(n => n.Close)
                        .FloatNumber(n => n.Low)
                        .FloatNumber(n => n.High)))
                .Settings(s => s.NumberOfShards(1).NumberOfReplicas(0))); // NOT PRODUCTION SETTINGS!!

            if (!newIndexResponse.IsValidResponse || newIndexResponse.Acknowledged is false)
                throw new Exception("Oh no!");

            var bulkAll = client.BulkAll(ReadStockData(), r => r
                .Index(IndexName)
                .BackOffRetries(20)
                .BackOffTime(TimeSpan.FromSeconds(10))
                .ContinueAfterDroppedDocuments()
                .DroppedDocumentCallback((r, d) => { Console.WriteLine(r.Error.Reason); })
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
            .Sort(srt => srt.Field(d => d.Date, c => c.Order(SortOrder.Desc))));

        if (!symbolResponse.IsValidResponse) throw new Exception("Oh no");

        foreach (var data in symbolResponse.Documents)
        {
            Console.WriteLine($"{data.Date:d}   {data.High:n2} {data.Low:n2}");
        }

        var fullTextSearchResponse = await client.SearchAsync<StockData>(s => s
            .Index(IndexName)
            .Query(q => q
                .Match(m => m.Field(f => f.Name).Query("inc")))
            .Size(20)
            .Sort(srt => srt.Field(d => d.Date, c => c.Order(SortOrder.Desc))));

        if (!fullTextSearchResponse.IsValidResponse) throw new Exception("Oh no");

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
                    .Order(new List<KeyValuePair<Field, SortOrder>> {new(Field.KeyField, SortOrder.Desc)})
                    .Aggregations(agg => agg
                        .Sum("trade-volumes", sum => sum.Field(fld => fld.Volume))))));

        if (!aggExampleResponse.IsValidResponse) throw new Exception("Oh no");

        var monthlyBuckets = aggExampleResponse.Aggregations
            .GetDateHistogram("by-month")
            .Buckets;

        foreach (var monthlyBucket in monthlyBuckets)
        {
            var volume = monthlyBucket.GetSum("trade-volumes").Value;
            Console.WriteLine($"{DateTimeOffset.FromUnixTimeMilliseconds(monthlyBucket.Key):yyyy-MM} : {volume:n0}");
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