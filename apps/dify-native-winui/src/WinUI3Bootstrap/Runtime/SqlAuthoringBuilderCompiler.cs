using System.Globalization;
using System.Text;

namespace AIWF.Native.Runtime;

public static class SqlAuthoringBuilderCompiler
{
    public static string BuildSql(SqlBuilderDraft draft, SqlConnectionProfile profile)
    {
        var table = BuildQualifiedTable(profile.NormalizedSourceType, draft.Schema, draft.Table);
        if (string.IsNullOrWhiteSpace(table))
        {
            return string.Empty;
        }

        var selectParts = BuildSelectParts(draft);
        var selectKeyword = BuildSelectKeyword(profile.NormalizedSourceType, draft.Limit);
        var sql = new StringBuilder();
        sql.Append(selectKeyword);
        sql.Append(' ');
        sql.Append(selectParts.Count == 0 ? "*" : string.Join(", ", selectParts));
        sql.Append(" FROM ");
        sql.Append(table);

        foreach (var join in draft.Joins.Where(static item => item.Enabled && !string.IsNullOrWhiteSpace(item.Table)))
        {
            var joinTable = BuildQualifiedTable(profile.NormalizedSourceType, join.Schema, join.Table);
            if (string.IsNullOrWhiteSpace(joinTable))
            {
                continue;
            }

            sql.Append(' ');
            sql.Append(RenderJoinType(join.JoinType));
            sql.Append(" JOIN ");
            sql.Append(joinTable);
            sql.Append(" ON ");
            sql.Append(table);
            sql.Append('.');
            sql.Append(QuoteIdentifier(profile.NormalizedSourceType, join.LeftField));
            sql.Append(" = ");
            sql.Append(joinTable);
            sql.Append('.');
            sql.Append(QuoteIdentifier(profile.NormalizedSourceType, join.RightField));
        }

        var whereParts = draft.Filters
            .Where(static filter => !string.IsNullOrWhiteSpace(filter.Field) && !string.IsNullOrWhiteSpace(filter.Operator))
            .Select(filter =>
                $"{table}.{QuoteIdentifier(profile.NormalizedSourceType, filter.Field)} {NormalizeOperator(filter.Operator)} {RenderLiteral(filter.Value)}")
            .ToArray();
        if (whereParts.Length > 0)
        {
            sql.Append(" WHERE ");
            sql.Append(string.Join(" AND ", whereParts));
        }

        var groupBy = draft.GroupByFields
            .Where(static field => !string.IsNullOrWhiteSpace(field))
            .Select(field => $"{table}.{QuoteIdentifier(profile.NormalizedSourceType, field)}")
            .ToArray();
        if (groupBy.Length > 0)
        {
            sql.Append(" GROUP BY ");
            sql.Append(string.Join(", ", groupBy));
        }

        if (!string.IsNullOrWhiteSpace(draft.OrderByField))
        {
            sql.Append(" ORDER BY ");
            sql.Append($"{table}.{QuoteIdentifier(profile.NormalizedSourceType, draft.OrderByField)}");
            sql.Append(draft.OrderByDescending ? " DESC" : " ASC");
        }

        if (profile.NormalizedSourceType == SqlConnectionProfile.Sqlite && draft.Limit > 0)
        {
            sql.Append(" LIMIT ");
            sql.Append(draft.Limit.ToString(CultureInfo.InvariantCulture));
        }

        return sql.ToString().Trim();
    }

    private static IReadOnlyList<string> BuildSelectParts(SqlBuilderDraft draft)
    {
        var parts = new List<string>();
        if (draft.Aggregates.Count == 0)
        {
            parts.AddRange(draft.SelectFields.Where(static field => !string.IsNullOrWhiteSpace(field)));
        }
        else
        {
            parts.AddRange(draft.GroupByFields.Where(static field => !string.IsNullOrWhiteSpace(field)));
            foreach (var aggregate in draft.Aggregates.Where(static item => !string.IsNullOrWhiteSpace(item.Function)))
            {
                var field = string.IsNullOrWhiteSpace(aggregate.Field) ? "*" : aggregate.Field.Trim();
                var alias = string.IsNullOrWhiteSpace(aggregate.Alias)
                    ? $"{aggregate.Function.ToLowerInvariant()}_{field.Replace("*", "all", StringComparison.Ordinal)}"
                    : aggregate.Alias.Trim();
                parts.Add($"{aggregate.Function.ToUpperInvariant()}({field}) AS {alias}");
            }
        }

        return parts
            .Select(static field => field.Trim())
            .Where(static field => !string.IsNullOrWhiteSpace(field))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string BuildSelectKeyword(string sourceType, int limit)
    {
        if (sourceType == SqlConnectionProfile.SqlServer && limit > 0)
        {
            return $"SELECT TOP {limit}";
        }

        return "SELECT";
    }

    private static string BuildQualifiedTable(string sourceType, string schema, string table)
    {
        var trimmedTable = table?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(trimmedTable))
        {
            return string.Empty;
        }

        var trimmedSchema = schema?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(trimmedSchema))
        {
            return QuoteIdentifier(sourceType, trimmedTable);
        }

        return $"{QuoteIdentifier(sourceType, trimmedSchema)}.{QuoteIdentifier(sourceType, trimmedTable)}";
    }

    private static string QuoteIdentifier(string sourceType, string value)
    {
        var trimmed = value?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            return string.Empty;
        }

        return sourceType == SqlConnectionProfile.SqlServer
            ? $"[{trimmed.Replace("]", "]]", StringComparison.Ordinal)}]"
            : $"\"{trimmed.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
    }

    private static string RenderJoinType(string joinType)
    {
        return string.Equals(joinType?.Trim(), "left", StringComparison.OrdinalIgnoreCase)
            ? "LEFT"
            : string.Equals(joinType?.Trim(), "right", StringComparison.OrdinalIgnoreCase)
                ? "RIGHT"
                : string.Equals(joinType?.Trim(), "full", StringComparison.OrdinalIgnoreCase)
                    ? "FULL"
                    : "INNER";
    }

    private static string NormalizeOperator(string value)
    {
        return value?.Trim() switch
        {
            "!=" => "!=",
            ">" => ">",
            ">=" => ">=",
            "<" => "<",
            "<=" => "<=",
            "LIKE" or "like" => "LIKE",
            _ => "=",
        };
    }

    private static string RenderLiteral(string value)
    {
        var trimmed = value?.Trim() ?? string.Empty;
        if (decimal.TryParse(trimmed, NumberStyles.Float, CultureInfo.InvariantCulture, out _))
        {
            return trimmed;
        }

        return $"'{trimmed.Replace("'", "''", StringComparison.Ordinal)}'";
    }
}
