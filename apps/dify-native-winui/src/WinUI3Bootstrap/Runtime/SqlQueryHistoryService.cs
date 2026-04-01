using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace AIWF.Native.Runtime;

public sealed record SqlQueryHistoryEntry(
    string Timestamp,
    string SqlText,
    string SourceType,
    string Database,
    int RowCount,
    bool Success,
    string? Name = null)
{
    public string DisplayText => string.IsNullOrWhiteSpace(Name)
        ? $"[{Timestamp}] {(Success ? "OK" : "ERR")} ({RowCount} rows) {Truncate(SqlText, 60)}"
        : $"★ {Name} — {Truncate(SqlText, 50)}";

    private static string Truncate(string value, int max) =>
        value.Length <= max ? value.Replace('\n', ' ') : value[..(max - 1)].Replace('\n', ' ') + "…";
}

public sealed class SqlQueryHistoryService
{
    private static readonly string HistoryFilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "AIWF", "sql-history.json");

    private static readonly string SavedQueriesFilePath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
        "AIWF", "sql-saved-queries.json");

    private const int MaxHistoryEntries = 100;

    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        WriteIndented = true,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    public List<SqlQueryHistoryEntry> History { get; private set; } = [];
    public List<SqlQueryHistoryEntry> SavedQueries { get; private set; } = [];

    public void Load()
    {
        History = LoadFile(HistoryFilePath);
        SavedQueries = LoadFile(SavedQueriesFilePath);
    }

    public void AddHistoryEntry(string sqlText, string sourceType, string database, int rowCount, bool success)
    {
        var entry = new SqlQueryHistoryEntry(
            Timestamp: DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
            SqlText: sqlText,
            SourceType: sourceType,
            Database: database,
            RowCount: rowCount,
            Success: success);

        History.Insert(0, entry);
        if (History.Count > MaxHistoryEntries)
        {
            History.RemoveRange(MaxHistoryEntries, History.Count - MaxHistoryEntries);
        }

        SaveFile(HistoryFilePath, History);
    }

    public void SaveQuery(string name, string sqlText, string sourceType, string database)
    {
        var existing = SavedQueries.FindIndex(q =>
            string.Equals(q.Name, name, StringComparison.OrdinalIgnoreCase));
        var entry = new SqlQueryHistoryEntry(
            Timestamp: DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
            SqlText: sqlText,
            SourceType: sourceType,
            Database: database,
            RowCount: 0,
            Success: true,
            Name: name);

        if (existing >= 0)
        {
            SavedQueries[existing] = entry;
        }
        else
        {
            SavedQueries.Insert(0, entry);
        }

        SaveFile(SavedQueriesFilePath, SavedQueries);
    }

    public void DeleteSavedQuery(int index)
    {
        if (index >= 0 && index < SavedQueries.Count)
        {
            SavedQueries.RemoveAt(index);
            SaveFile(SavedQueriesFilePath, SavedQueries);
        }
    }

    private static List<SqlQueryHistoryEntry> LoadFile(string path)
    {
        try
        {
            if (!File.Exists(path))
            {
                return [];
            }

            var json = File.ReadAllText(path, Encoding.UTF8);
            return JsonSerializer.Deserialize<List<SqlQueryHistoryEntry>>(json, JsonOptions) ?? [];
        }
        catch
        {
            return [];
        }
    }

    private static void SaveFile(string path, List<SqlQueryHistoryEntry> entries)
    {
        try
        {
            var dir = Path.GetDirectoryName(path) ?? ".";
            Directory.CreateDirectory(dir);
            File.WriteAllText(path, JsonSerializer.Serialize(entries, JsonOptions), Encoding.UTF8);
        }
        catch
        {
        }
    }
}
