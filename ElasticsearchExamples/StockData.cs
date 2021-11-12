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

    public StockData(string dataLine)
    {
        var columns = dataLine.Split(',', StringSplitOptions.TrimEntries);

        if (DateTime.TryParse(columns[0], out var date))
            Date = date;

        if (double.TryParse(columns[1], out var open))
            Open = open;

        if (double.TryParse(columns[2], out var high))
            High = high;

        if (double.TryParse(columns[3], out var low))
            Low = low;

        if (double.TryParse(columns[4], out var close))
            Close = close;

        if (uint.TryParse(columns[5], out var volume))
            Volume = volume;

        Symbol = columns[6];

        if (CompanyLookup.TryGetValue(Symbol, out var name))
            Name = name;
    }

    public DateTime Date { get; init; }
    public double Open { get; init; }
    public double Close { get; init; }
    public double High { get; init; }
    public double Low { get; init; }
    public uint Volume { get; init; }
    public string Symbol { get; init; }
    public string Name { get; init; }
}