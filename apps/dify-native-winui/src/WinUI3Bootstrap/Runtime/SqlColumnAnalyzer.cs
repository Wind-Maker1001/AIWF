using System.Globalization;

namespace AIWF.Native.Runtime;

public sealed record SqlColumnStats(
    string ColumnName,
    int RowCount,
    int NullCount,
    int DistinctCount,
    string? MinValue,
    string? MaxValue,
    double? NumericMean,
    double? NumericSum)
{
    public string Summary =>
        NumericMean.HasValue
            ? $"{ColumnName}: {RowCount} rows, {NullCount} null, {DistinctCount} distinct | min={MinValue} max={MaxValue} avg={NumericMean:F2} sum={NumericSum:F2}"
            : $"{ColumnName}: {RowCount} rows, {NullCount} null, {DistinctCount} distinct | min={MinValue} max={MaxValue}";
}

public static class SqlColumnAnalyzer
{
    public static IReadOnlyList<SqlColumnStats> Analyze(
        IReadOnlyList<string> columns,
        IReadOnlyList<IReadOnlyList<string>> rows)
    {
        var stats = new List<SqlColumnStats>();
        for (var colIdx = 0; colIdx < columns.Count; colIdx++)
        {
            var values = new List<string>();
            var nullCount = 0;
            var numericValues = new List<double>();

            for (var rowIdx = 0; rowIdx < rows.Count; rowIdx++)
            {
                var cell = colIdx < rows[rowIdx].Count ? rows[rowIdx][colIdx] : string.Empty;
                if (string.IsNullOrEmpty(cell) || string.Equals(cell, "null", StringComparison.OrdinalIgnoreCase))
                {
                    nullCount++;
                }
                else
                {
                    values.Add(cell);
                    if (double.TryParse(cell, NumberStyles.Float, CultureInfo.InvariantCulture, out var num))
                    {
                        numericValues.Add(num);
                    }
                }
            }

            var distinct = values.Distinct(StringComparer.OrdinalIgnoreCase).Count();
            var sorted = values.OrderBy(v => v, StringComparer.OrdinalIgnoreCase).ToList();

            stats.Add(new SqlColumnStats(
                ColumnName: columns[colIdx],
                RowCount: rows.Count,
                NullCount: nullCount,
                DistinctCount: distinct,
                MinValue: sorted.Count > 0 ? sorted[0] : null,
                MaxValue: sorted.Count > 0 ? sorted[^1] : null,
                NumericMean: numericValues.Count > 0 ? numericValues.Average() : null,
                NumericSum: numericValues.Count > 0 ? numericValues.Sum() : null));
        }

        return stats;
    }
}
