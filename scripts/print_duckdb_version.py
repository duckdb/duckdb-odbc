#!/usr/bin/env python
# -*- coding: utf-8 -*-
# Prints the short commit hash of the DuckDB revision the driver is built against, see vendor.py.

import re
from os import path
import sys

project_dir = path.dirname(path.dirname(path.abspath(__file__)))
revision_path = path.join(project_dir, "third_party", "duckdb_cpp", "duckdb_revision.cmake")

with open(revision_path, "r") as fd:
    match = re.search(r'set\(DUCKDB_GIT_SHA "([0-9a-f]+)"\)', fd.read())
    if match is not None:
        print(match.group(1)[:10], end="")
        sys.exit(0)

    print("ERROR: DuckDB version not found", file=sys.stderr)
    sys.exit(1)
