import os, subprocess
from os import path

script_dir = path.dirname(path.realpath(__file__))
project_dir = path.dirname(script_dir)
reg_exe = path.join(os.environ["WINDIR"], "System32", "reg.exe")

subprocess.run(
    [
        reg_exe,
        "add",
        "HKCU\\SOFTWARE\\ODBC\\ODBC.INI\\DuckDB",
        "/v",
        "database",
        "/t",
        "REG_SZ",
        "/d",
        path.join(project_dir, "test", "sql", "storage_version", "storage_version.db"),
    ],
    check=True,
)
subprocess.run(
    [reg_exe, "add", "HKCU\\SOFTWARE\\ODBC\\ODBC.INI\\DuckDB", "/v", "access_mode", "/t", "REG_SZ", "/d", "READ_ONLY"],
    check=True,
)
subprocess.run(
    [
        reg_exe,
        "add",
        "HKCU\\SOFTWARE\\ODBC\\ODBC.INI\\DuckDB",
        "/v",
        "allow_unsigned_extensions",
        "/t",
        "REG_SZ",
        "/d",
        "true",
    ],
    check=True,
)
