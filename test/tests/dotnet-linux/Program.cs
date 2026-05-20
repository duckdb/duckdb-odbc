using System.Data.Odbc;

class Program
{
    public static void AssertEquals(string expected, string actual)
    {
        if (!string.Equals(expected, actual, StringComparison.Ordinal))
        {
            throw new Exception($"Assertion failed. Expected: '{expected}', Actual: '{actual}'");
        }
    }

    static void CheckFileldType(OdbcConnection conn, string sql, string expectedDataType)
    {
        using (OdbcCommand command = conn.CreateCommand())
        {
            command.CommandText = sql;
            using (OdbcDataReader reader = command.ExecuteReader())
            {
                reader.Read();
                AssertEquals(expectedDataType, reader.GetFieldType(0).ToString());
            }
        }
    }

    static void TestFieldTypes(OdbcConnection conn)
    {
        // Integer types
        CheckFileldType(conn, "SELECT 1::TINYINT", "System.Int16"); // SByte in OleDB
        CheckFileldType(conn, "SELECT 1::UTINYINT", "System.Byte");
        CheckFileldType(conn, "SELECT 1::SMALLINT", "System.Int16");
        CheckFileldType(conn, "SELECT 1::USMALLINT", "System.Int32"); // UInt16 in OleDB
        CheckFileldType(conn, "SELECT 1::INTEGER", "System.Int32");
        CheckFileldType(conn, "SELECT 1::UINTEGER", "System.Int64"); // Uint32 in OldDb
        CheckFileldType(conn, "SELECT 1::BIGINT", "System.Int64");
        CheckFileldType(conn, "SELECT 1::UBIGINT", "System.Decimal"); // Uint64 in OleDb
        CheckFileldType(conn, "SELECT 1::HUGEINT", "System.Decimal");
        CheckFileldType(conn, "SELECT 1::UHUGEINT", "System.Decimal");

        // Decimal types
        CheckFileldType(conn, "SELECT '1'::DECIMAL(18,3)", "System.Decimal");
        CheckFileldType(conn, "SELECT '1'::NUMERIC(18,3)", "System.Decimal");

        // Boolean types
        CheckFileldType(conn, "SELECT 1::BIT", "System.Boolean");
        CheckFileldType(conn, "SELECT true::BOOLEAN", "System.String");

        // Date time types
        CheckFileldType(conn, "SELECT '1970-01-01'::Date", "System.DateTime");
        CheckFileldType(conn, "SELECT '12:34'::TIME", "System.TimeSpan");
        CheckFileldType(conn, "SELECT '2020-01-02 12:34:45+01'::TIMESTAMP WITH TIME ZONE", "System.DateTime");
        CheckFileldType(conn, "SELECT '2020-01-02 12:34:45'::TIMESTAMP WITHOUT TIME ZONE", "System.DateTime");
        CheckFileldType(conn, "SELECT '1 day'::INTERVAL", "System.String");

        // Other types
        CheckFileldType(conn, "SELECT ''::BLOB", "System.Byte[]");
        CheckFileldType(conn, "SELECT '{}'::JSON", "System.String");
        CheckFileldType(conn, "SELECT '516d4db7-075d-419d-8d25-49dc4d7946aa'::UUID", "System.String");
        CheckFileldType(conn, "SELECT ''::VARCHAR", "System.String");
    }

    static void ExecuteNonQueryOdbcCommand(OdbcConnection conn, string sql)
    {
        using (var cmd = new OdbcCommand(sql, conn))
        {
            cmd.ExecuteNonQuery();
        }
    }

    // see https://github.com/duckdb/duckdb-odbc/issues/440
    static void TestPrepareExecuteColAtrribute(OdbcConnection conn)
    {
        ExecuteNonQueryOdbcCommand(conn, "CREATE OR REPLACE TABLE big_table(id BIGINT, hash VARCHAR, rand DOUBLE, \"value\" DOUBLE);");
        ExecuteNonQueryOdbcCommand(conn, "PREPARE insert_stmt AS INSERT INTO big_table (id, hash, rand, value) VALUES (?,?,?,?);");
        ExecuteNonQueryOdbcCommand(conn, "BEGIN TRANSACTION;");
        ExecuteNonQueryOdbcCommand(conn, "EXECUTE insert_stmt(1,'704719932875954298',1.0,0.7095996033937675974);");
        ExecuteNonQueryOdbcCommand(conn, "COMMIT;");
    }


    static void Main(string[] args)
    {
        using (OdbcConnection conn = new OdbcConnection("Driver={DuckDB Driver}"))
        {
            conn.Open();
            TestFieldTypes(conn);
            TestPrepareExecuteColAtrribute(conn);
        }

        Console.WriteLine("dotnet-linux tests passed successfully");
    }
}