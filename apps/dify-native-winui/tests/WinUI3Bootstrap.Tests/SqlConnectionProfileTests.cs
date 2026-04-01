using AIWF.Native.Runtime;
using Xunit;

namespace AIWF.Native.Tests;

public sealed class SqlConnectionProfileTests
{
    [Fact]
    public void BuildRuntimeSource_UsesSqlitePathForSqliteProfiles()
    {
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.Sqlite,
            SQLitePath = @"D:\data\demo.db"
        };

        Assert.Equal(@"D:\data\demo.db", profile.BuildRuntimeSource());
    }

    [Fact]
    public void BuildRuntimeSource_UsesWindowsAuthWhenRequested()
    {
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.SqlServer,
            SqlServerHost = "db.local",
            SqlServerPort = "1444",
            Database = "AIWF",
            UseWindowsAuth = true,
            User = "svc",
            Password = "secret"
        };

        Assert.Equal("db.local:1444/AIWF?windows_auth=true", profile.BuildRuntimeSource());
    }

    [Fact]
    public void BuildRuntimeSource_UsesSqlCredentialsWhenWindowsAuthDisabled()
    {
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.SqlServer,
            SqlServerHost = "db.local",
            SqlServerPort = "1433",
            Database = "AIWF",
            UseWindowsAuth = false,
            User = "svc",
            Password = "p@ss&word"
        };

        Assert.Equal("db.local:1433/AIWF?user=svc&password=p%40ss%26word", profile.BuildRuntimeSource());
    }

    [Fact]
    public void BuildRuntimeSource_PostgresUsesDefaultPort5432()
    {
        var profile = SqlConnectionProfile.Default with
        {
            SourceType = SqlConnectionProfile.Postgres,
            SqlServerHost = "pg.local",
            Database = "analytics",
            User = "admin",
            Password = "secret",
            UseWindowsAuth = false,
        };

        Assert.Equal("pg.local:5432/analytics?user=admin&password=secret", profile.BuildRuntimeSource());
        Assert.Equal(SqlConnectionProfile.Postgres, profile.NormalizedSourceType);
    }

    [Fact]
    public void NormalizedSourceType_RecognizesPostgres()
    {
        var profile = SqlConnectionProfile.Default with { SourceType = "Postgres" };
        Assert.Equal(SqlConnectionProfile.Postgres, profile.NormalizedSourceType);

        var profile2 = SqlConnectionProfile.Default with { SourceType = "POSTGRES" };
        Assert.Equal(SqlConnectionProfile.Postgres, profile2.NormalizedSourceType);
    }
}
