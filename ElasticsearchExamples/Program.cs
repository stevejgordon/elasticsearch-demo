using Nest;

namespace ElasticsearchExamples;

internal class Program
{
    private const string IndexName = "stock-demo-v1";

    public static IElasticClient Client = new ElasticClient(new ConnectionSettings().DefaultIndex(IndexName));

    private static async Task Main(string[] args)
    {
        var existsResponse = await Client.Indices.ExistsAsync(IndexName);

        if (!existsResponse.Exists)
        {
            var newIndexResponse = await Client.Indices.CreateAsync(IndexName, i => i
                .Map(m => m
                    .AutoMap<StockData>()
                    .Properties<StockData>(p => p.Keyword(k => k.Name(n => n.Symbol))))
                .Settings(s => s.NumberOfShards(1).NumberOfReplicas(1)));

            if (!newIndexResponse.IsValid || newIndexResponse.Acknowledged is false) throw new Exception("Oh no!");

            var bulkAll = Client.BulkAll(ReadStockData(), r => r
                .Index(IndexName)
                .BackOffRetries(2)
                .BackOffTime("30s")
                .MaxDegreeOfParallelism(4)
                .Size(1000));

            bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));
        }

        var symbolResponse = await Client.SearchAsync<StockData>(s => s
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
            //Console.WriteLine($"{data.Date}   {data.High} {data.Low}");
        }

        var fullTextSearchResponse = await Client.SearchAsync<StockData>(s => s.Index(IndexName)
            .Query(q => q
                .Match(m => m.Field(f => f.Name).Query("inc")))
            .Size(20)
            .Sort(srt => srt.Descending(d => d.Date)));

        foreach (var data in fullTextSearchResponse.Documents)
        {
            //Console.WriteLine($"{data.Name} {data.Date}   {data.High} {data.Low}");
        }

        var aggExampleResponse = await Client.SearchAsync<StockData>(s => s
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

        var monthlyBuckets = aggExampleResponse.Aggregations.DateHistogram("by-month").Buckets;

        foreach (var monthlyBucket in monthlyBuckets)
        {
            var volume = monthlyBucket.Sum("trade-volumes").Value;
            //Console.WriteLine($"{monthlyBucket.Date} : {volume:n0}");
        }

        var scrollAllObservable = Client.ScrollAll<StockData>("10s", Environment.ProcessorCount, scroll => scroll
            .Search(s => s.Index(IndexName).MatchAll().Size(100))
            .MaxDegreeOfParallelism(Environment.ProcessorCount));

        scrollAllObservable.Wait(TimeSpan.FromMinutes(5), s =>
        {
            foreach (var doc in s.SearchResponse.Documents)
            {
                Console.WriteLine(doc.Symbol);
            }
        });
    }

    public static IEnumerable<StockData> ReadStockData()
    {
        var file = new StreamReader("c:\\stock-data\\all_stocks_5yr.csv");

        string line;
        while ((line = file.ReadLine()) is not null)
        {
            yield return new StockData(line);
        }
    }
}
