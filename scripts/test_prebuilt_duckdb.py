#!/usr/bin/env python3
"""Test archive lookup and fallback without network access or a DuckDB build."""

import io
from pathlib import Path
import subprocess
import tarfile
import tempfile
import unittest


MODULE = Path(__file__).resolve().parents[1] / "cmake" / "PrebuiltDuckDB.cmake"
COMMIT = "23221edfd5"
VERSION = "v2.1.0-alpha54418"
LIBRARIES = (
    "duckdb_static",
    "duckdb_httplib",
    "duckdb_loadable_extensions",
    "core_functions_extension",
    "parquet_extension",
    "icu_extension",
    "json_extension",
)


class PrebuiltDuckDBTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.staging = self.root / "staging"
        self.version_file = self.staging / COMMIT / "latest_alpha_version.txt"
        self.version_file.parent.mkdir(parents=True)
        self.version_file.write_text(VERSION + "\n")
        version_source = (
            self.root / "src/duckdb/src/function/table/version/pragma_version.cpp"
        )
        version_source.parent.mkdir(parents=True)
        version_source.write_text(f'#define DUCKDB_SOURCE_ID "{COMMIT}"\n')
        self.probe = self.root / "probe.cmake"

    def archive(self, platform="linux-amd64", omit=(), version=VERSION):
        destination = self.staging / COMMIT / version / "duckdb/duckdb/github_release"
        destination.mkdir(parents=True, exist_ok=True)
        archive = destination / f"duckdb-static-libs-{platform}.tar.gz"
        with tarfile.open(archive, "w:gz") as target:
            for library in LIBRARIES:
                if library in omit:
                    continue
                name = (
                    f"{library}.lib"
                    if platform.startswith("windows-")
                    else f"lib{library}.a"
                )
                member = tarfile.TarInfo(name)
                member.size = 8
                target.addfile(member, io.BytesIO(b"!<arch>\n"))
        return archive

    def lookup(self, **overrides):
        build = self.root / "build"
        build.mkdir(exist_ok=True)
        variables = {
            "PROJECT_SOURCE_DIR": self.root.as_posix(),
            "PROJECT_BINARY_DIR": build.as_posix(),
            "CMAKE_BUILD_TYPE": "Release",
            "CMAKE_SYSTEM_NAME": "Linux",
            "CMAKE_SYSTEM_PROCESSOR": "x86_64",
            "APPLE": "OFF",
            "MSVC": "OFF",
            "ODBC_DUCKDB_STAGING_URL": self.staging.as_uri(),
        }
        variables.update(overrides)
        # Set normal variables inside the script: CMake's host platform
        # variables take precedence over command-line cache variables.
        self.probe.write_text(
            "cmake_minimum_required(VERSION 3.5...3.29)\n"
            + "".join(f'set({name} "{value}")\n' for name, value in variables.items())
            + f'include("{MODULE.as_posix()}")\n'
            + 'file(WRITE "${PROJECT_BINARY_DIR}/libraries.txt" '
            '"${DUCKDB_PREBUILT_LIBRARIES}")\n'
        )
        result = subprocess.run(
            ["cmake", "-P", str(self.probe)],
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
        return (build / "libraries.txt").read_text(), result.stdout

    def test_supported_targets(self):
        targets = (
            ("linux-amd64", {}),
            ("linux-arm64", {"CMAKE_SYSTEM_PROCESSOR": "aarch64"}),
            ("osx-arm64", {"APPLE": "ON", "CMAKE_SYSTEM_PROCESSOR": "arm64"}),
            # Cross compilation must select the target, not the host archive.
            (
                "osx-amd64",
                {
                    "APPLE": "ON",
                    "CMAKE_SYSTEM_PROCESSOR": "arm64",
                    "CMAKE_OSX_ARCHITECTURES": "x86_64",
                },
            ),
            (
                "windows-amd64",
                {"MSVC": "ON", "CMAKE_CXX_COMPILER_ARCHITECTURE_ID": "x64"},
            ),
            (
                "windows-arm64",
                {"MSVC": "ON", "CMAKE_CXX_COMPILER_ARCHITECTURE_ID": "ARM64"},
            ),
        )
        for platform, variables in targets:
            with self.subTest(platform=platform):
                self.archive(platform)
                libraries, output = self.lookup(**variables)
                self.assertIn(f"/{COMMIT}/{VERSION}/{platform}/", libraries)
                self.assertIn("Using staged DuckDB", output)

    def test_missing_version_falls_back(self):
        self.version_file.unlink()
        libraries, output = self.lookup()
        self.assertEqual(libraries, "")
        self.assertIn("building from source", output)

    def test_missing_archive_falls_back(self):
        libraries, output = self.lookup()
        self.assertEqual(libraries, "")
        self.assertIn("static archive unavailable", output)

    def test_invalid_versions_fall_back(self):
        for version in ("", "../other-version", "an error page"):
            with self.subTest(version=version):
                self.version_file.write_text(version)
                libraries, output = self.lookup()
                self.assertEqual(libraries, "")
                self.assertIn("Invalid staged DuckDB version", output)

    def test_incomplete_archive_falls_back(self):
        self.archive(omit=("parquet_extension",))
        libraries, output = self.lookup()
        self.assertEqual(libraries, "")
        self.assertIn("missing parquet_extension", output)
        self.assertFalse(list((self.root / "build").rglob("complete")))

    def test_corrupt_archive_falls_back(self):
        self.archive().write_bytes(b"not a tar archive")
        libraries, output = self.lookup()
        self.assertEqual(libraries, "")
        self.assertIn("Cannot extract", output)

    def test_debug_disabled_and_unsupported_builds_skip_lookup(self):
        self.version_file.unlink()
        for variables in (
            {"CMAKE_BUILD_TYPE": "Debug"},
            {"ODBC_USE_PREBUILT_DUCKDB": "OFF"},
            {"CLANG_TIDY": "ON"},
            {"CMAKE_SYSTEM_PROCESSOR": "riscv64"},
            {"APPLE": "ON", "CMAKE_OSX_ARCHITECTURES": "arm64;x86_64"},
            {"CMAKE_SYSTEM_NAME": "FreeBSD"},
        ):
            with self.subTest(variables=variables):
                libraries, output = self.lookup(**variables)
                self.assertEqual(libraries, "")
                self.assertNotIn("No staged DuckDB version", output)

    def test_cached_archive_is_reused(self):
        archive = self.archive()
        libraries, _ = self.lookup()
        archive.unlink()
        cached_libraries, output = self.lookup()
        self.assertEqual(cached_libraries, libraries)
        self.assertNotIn("Downloading", output)

    def test_restage_uses_new_version(self):
        self.archive()
        old_libraries, _ = self.lookup()
        new_version = "v2.1.0-alpha54419"
        self.version_file.write_text(new_version)
        self.archive(version=new_version)
        libraries, _ = self.lookup()
        self.assertNotEqual(libraries, old_libraries)
        self.assertIn(new_version, libraries)


if __name__ == "__main__":
    unittest.main()
