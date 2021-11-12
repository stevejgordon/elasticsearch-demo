namespace ElasticsearchExamples;

internal class Program
{
    private const string IndexName = "stock-demo-v1";

    // TODO: Create client

    private static void Main(string[] args)
    {
        // TODO: Check for existing index

        // TODO: Create and seed index if it does not exist

        // TODO: Search for term "MSFT"

        // TODO: Search to query for data where name contains "inc"

        // TODO: Search and aggregate on trade volumes by month

        // TODO: Scroll all
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
