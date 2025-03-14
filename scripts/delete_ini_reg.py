import os, subprocess
from os import path

subprocess.run(
    [path.join(os.environ["WINDIR"], "System32", "reg.exe"), "delete", "HKCU\\SOFTWARE\\ODBC\\ODBC.INI\\DuckDB", "/f"],
    check=True,
)
