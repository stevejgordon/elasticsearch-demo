using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.Aggregations;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Clients.Elasticsearch.Mapping;
using Elastic.Transport;

namespace ElasticsearchExamples;

internal class Version8
{
    public static async Task ExecuteAsync()
    {
        const string IndexName = "stock-demo-v8";
        const string CloudId = "TODO";

        #region 'panic mode' docker fallabck

        //var settings = new ElasticsearchClientSettings(new Uri("https://localhost:9200"))
        //    // Replace this with the fingerprint from the local server
        //    .CertificateFingerprint("E4:21:7E:BF:F4:62:8B:C5:72:9C:D0:A8:51:D9:8C:65:E4:FF:28:96:1A:32:EE:F4:6D:BE:00:17:48:02:1C:54")
        //    .Authentication(new BasicAuthentication("elastic", "password"));
        //var client = new ElasticsearchClient(settings);

        #endregion

        // DON'T EXPOSE PASSWORDS IN REAL APPS!!
        var client = new ElasticsearchClient(CloudId,
            new BasicAuthentication("elastic", "TODO"));

        var existsResponse = await client.Indices.ExistsAsync(IndexName);

        if (!existsResponse.Exists)
        {
            #region ObjectInitializer example

            // var floatProperty = new FloatNumberProperty();
            // var request = new CreateIndexRequest(IndexName)
            // {
            //     Mappings = new()
            //     {
            //         Properties = new(new Dictionary<PropertyName, IProperty>
            //         {
            //             { "symbol", new KeywordProperty() },
            //             { "open", floatProperty },
            //             { "close", floatProperty},
            //             { "low", floatProperty },
            //             { "high", floatProperty }
            //         })
            //     },
            //     Settings = new IndexSettings 
            //     {
            //         NumberOfShards = 1,
            //         NumberOfReplicas = 0 
            //     }
            // };

            #endregion

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

            foreach(var stockData in ReadStockData())
                await client.IndexAsync(stockData, IndexName);

            // BULK INDEX ALL DATA

            var bulkAll = client.BulkAll(ReadStockData(), r => r
                .Index(IndexName)
                .BackOffRetries(20)
                .BackOffTime(TimeSpan.FromSeconds(10))
                .ContinueAfterDroppedDocuments()
                .DroppedDocumentCallback((r, d) => { Console.WriteLine(r.Error.Reason); })
                .MaxDegreeOfParallelism(4)
                .Size(5000));

            bulkAll.Wait(TimeSpan.FromMinutes(10), r => Console.WriteLine("Data indexed"));
        }

        // SCENARIO: GET 20 MOST RECENT STOCK DATA DOCUMENTS FOR 'MSFT' STOCKS.

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

        // SCENARIO: GET 20 MOST RECENT STOCK DATA DOCUMENTS FOR COMPANIES WHICH INCLUDE 'inc' IN THE NAME.

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

        // SCENARIO: GET TOTAL TRADE VOLUMES PER MONTH FOR 'MSFT' STOCK.

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
                    .Order(
                        new List<KeyValuePair<Field, SortOrder>>
                        {
                            new(Field.KeyField, SortOrder.Desc)
                        })
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

        static StockData GetSingleStockItem()
        {
            var file = new StreamReader("c:\\stock-data\\all_stocks_5yr.csv");
            file.ReadLine(); // Skip the header
            var line = file.ReadLine(); // Read first stock data line
            return StockData.ParseFromFileLine(line);
        }
    }
}