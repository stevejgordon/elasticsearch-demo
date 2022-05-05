using Elasticsearch.Net;
using Nest;

namespace ElasticsearchExamples;

internal class Version7
{
    public static async Task ExecuteAsync()
    {
        const string IndexName = "stock-demo-v7";

        //var settings = new ConnectionSettings(new Uri("https://localhost:9200"))
        //    .CertificateFingerprint("E8:76:3D:91:81:8C:57:31:6F:2F:E0:4C:17:78:78:FB:38:CC:37:27:41:7A:94:B4:12:AA:B6:D1:D6:C4:4C:7D")
        //    .BasicAuthentication("elastic", "password")
        //    .EnableApiVersioningHeader();

        var client = new ElasticClient("CLOUDID", new BasicAuthenticationCredentials("elastic", "password"));

        var existsResponse = await client.Indices.ExistsAsync(IndexName);

        if (!existsResponse.Exists)
        {
            var newIndexResponse = await client.Indices.CreateAsync(IndexName, i => i
                .Map(m => m
                    .AutoMap<StockData>()
                    .Properties<StockData>(p => p
                        .Keyword(k => k.Name(n => n.Symbol))
                        .Number(n => n.Name(n => n.High).Type(NumberType.Float))
                        .Number(n => n.Name(n => n.Low).Type(NumberType.Float))
                        .Number(n => n.Name(n => n.Open).Type(NumberType.Float))
                        .Number(n => n.Name(n => n.Close).Type(NumberType.Float))))
                .Settings(s => s.NumberOfShards(1).NumberOfReplicas(1)));

            if (!newIndexResponse.IsValid || newIndexResponse.Acknowledged is false) throw new Exception("Oh no!");

            var bulkAll = client.BulkAll(ReadStockData(), r => r
                .Index(IndexName)
                .BackOffRetries(2)
                .BackOffTime("30s")
                .MaxDegreeOfParallelism(4)
                .Size(1000));

            bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));
        }

        var symbolResponse = await client.SearchAsync<StockData>(s => s
            .Index(IndexName)
            .Query(q => q
                .Bool(b => b
                    .Filter(f => f
                        .Term(t => t.Field(fld => fld.Symbol).Value("MSFT")))))
            .Size(20)
            .Sort(srt => srt.Descending(d => d.Date)));

        if (!symbolResponse.IsValid) throw new Exception("Oh no");

        foreach (var data in symbolResponse.Documents)
        {
            Console.WriteLine($"{data.Date}   {data.High} {data.Low}");
        }

        var fullTextSearchResponse = await client.SearchAsync<StockData>(s => s.Index(IndexName)
            .Query(q => q
                .Match(m => m.Field(f => f.Name).Query("inc")))
            .Size(20)
            .Sort(srt => srt.Descending(d => d.Date)));

        if (!fullTextSearchResponse.IsValid) throw new Exception("Oh no");

        foreach (var data in fullTextSearchResponse.Documents)
        {
            Console.WriteLine($"{data.Name} {data.Date}   {data.High} {data.Low}");
        }

        var aggExampleResponse = await client.SearchAsync<StockData>(s => s
            .Index(IndexName)
            .Size(0)
            .Query(q => q
                .Bool(b => b
                    .Filter(f => f
                        .Term(t => t.Field(fld => fld.Symbol).Value("MSFT")))))
            .Aggregations(a => a
                .DateHistogram("by-month", dh => dh
                    .CalendarInterval(DateInterval.Month)
                    .Field(fld => fld.Date)
                    .Order(HistogramOrder.KeyDescending)
                    .Aggregations(agg => agg
                        .Sum("trade-volumes", sum => sum.Field(fld => fld.Volume))))));

        if (!aggExampleResponse.IsValid) throw new Exception("Oh no");

        var monthlyBuckets = aggExampleResponse.Aggregations.DateHistogram("by-month").Buckets;

        foreach (var monthlyBucket in monthlyBuckets)
        {
            var volume = monthlyBucket.Sum("trade-volumes").Value;
            Console.WriteLine($"{monthlyBucket.Date} : {volume:n0}");
        }

        //var scrollAllObservable = client.ScrollAll<StockData>("10s", Environment.ProcessorCount, scroll => scroll
        //    .Search(s => s.Index(IndexName).MatchAll().Size(100))
        //    .MaxDegreeOfParallelism(Environment.ProcessorCount));

        //scrollAllObservable.Wait(TimeSpan.FromMinutes(5), s =>
        //{
        //    foreach (var doc in s.SearchResponse.Documents)
        //    {
        //        Console.WriteLine(doc.Symbol);
        //    }
        //});

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
