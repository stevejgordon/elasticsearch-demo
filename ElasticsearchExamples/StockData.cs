namespace ElasticsearchExamples;

public class StockData
{
    private static readonly Dictionary<string, string> CompanyLookup = new()
    {
        { "AAL", "American Airlines Group Inc" },
        { "MSFT", "Microsoft Corporation" },
        { "AME", "AMETEK, Inc." },
        { "M", "Macy's inc" }
    };

    public DateTime Date { get; init; }
    public double Open { get; init; }
    public double Close { get; init; }
    public double High { get; init; }
    public double Low { get; init; }
    public int Volume { get; init; }
    public string Symbol { get; init; }
    public string Name { get; init; }

    public static StockData ParseFromFileLine(string dataLine)
    {
        var columns = dataLine.Split(',', StringSplitOptions.TrimEntries);

        var date = DateTime.Parse(columns[0]);

        _ = float.TryParse(columns[1], out float open);
        _ = float.TryParse(columns[1], out float high);
        _ = float.TryParse(columns[1], out float low);
        _ = float.TryParse(columns[1], out float close);

        var volume = int.Parse(columns[5]);
        var symbol = columns[6];

        CompanyLookup.TryGetValue(symbol, out string name);

        return new StockData
        {
            Name = name,
            Date = date,
            Open = open,
            Close = close,
            High = high,
            Low = low,
            Volume = volume,
            Symbol = symbol
        };
    }
}