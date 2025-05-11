System.Data.Odbc tests for .NET on Linux
----------------------------------------

To run tests on Ubuntu-22.04 add `[DuckDB Driver]` to `odbcinst.ini` and run:

```
sudo apt install unixodbc-dev dotnet-sdk-8.0
dotnet run
```

On newer versions of Linux the version of `TargetFramework` in `dotnet.csproj` may need to be corrected to match installed SDK.
