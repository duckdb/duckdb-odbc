"""Builds and installs the DuckDB engine the ODBC driver links.

The driver is compiled against the vendored stable C++ API package in third_party/duckdb_cpp, and statically links a
DuckDB engine built from the revision recorded in third_party/duckdb_cpp/duckdb_revision.cmake. This script fetches
that revision (or uses an existing checkout), builds it with the extensions the driver bundles, and installs it into a
prefix that the driver's CMake build picks up through DUCKDB_ROOT.

Usage:
    python scripts/build_duckdb.py --install-dir build/duckdb/install [--source-dir path/to/duckdb] [--build-type Release]
"""

import argparse
import os
import re
import shutil
import subprocess
import sys

PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REVISION_FILE = os.path.join(PROJECT_DIR, 'third_party', 'duckdb_cpp', 'duckdb_revision.cmake')
EXTENSIONS = 'core_functions;parquet;icu;json'
# What the driver registers when it starts: the extensions, the HTTP client and the loading of further extensions.
LOADER_COMPONENTS = ['httplib', 'loadable_extensions'] + EXTENSIONS.split(';')
# Where the generated static extension loader is installed, relative to the install prefix.
LOADER_FILE = os.path.join('share', 'duckdb_odbc', 'static_extension_loader.c')


def read_revision():
    with open(REVISION_FILE) as f:
        content = f.read()
    values = dict(re.findall(r'set\((DUCKDB_GIT_[A-Z]+) "([^"]*)"\)', content))
    return values['DUCKDB_GIT_REPOSITORY'], values['DUCKDB_GIT_SHA'], values['DUCKDB_GIT_VERSION']


def run(cmd, cwd=None):
    print('+ ' + ' '.join(cmd), flush=True)
    subprocess.check_call(cmd, cwd=cwd)


def fetch_source(source_dir, repository, sha):
    if not os.path.isdir(os.path.join(source_dir, '.git')):
        os.makedirs(source_dir, exist_ok=True)
        run(['git', 'init', '-q'], cwd=source_dir)
        run(['git', 'remote', 'add', 'origin', repository], cwd=source_dir)
    current = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=source_dir,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )
    if current.returncode == 0 and current.stdout.strip() == sha:
        return
    run(['git', 'fetch', '-q', '--depth', '1', 'origin', sha], cwd=source_dir)
    run(['git', 'checkout', '-q', '--detach', 'FETCH_HEAD'], cwd=source_dir)


def main():
    parser = argparse.ArgumentParser(description='Builds the DuckDB engine the ODBC driver links')
    parser.add_argument('--install-dir', required=True, help='Prefix to install DuckDB into')
    parser.add_argument('--source-dir', default=None, help='DuckDB checkout to build (default: fetch the pinned one)')
    parser.add_argument('--build-dir', default=None, help='Build directory (default: <install-dir>/../build)')
    parser.add_argument('--build-type', default='Release', help='CMake build type')
    parser.add_argument('--generator', default=None, help='CMake generator (default: Ninja when available)')
    parser.add_argument('--osx-architectures', default=None, help='CMAKE_OSX_ARCHITECTURES, e.g. "x86_64;arm64"')
    parser.add_argument('--cmake-arg', action='append', default=[], help='Extra argument passed to CMake configure')
    args = parser.parse_args()

    repository, sha, version = read_revision()
    install_dir = os.path.abspath(args.install_dir)
    base_dir = os.path.dirname(install_dir)
    source_dir = os.path.abspath(args.source_dir) if args.source_dir else os.path.join(base_dir, 'src')
    build_dir = os.path.abspath(args.build_dir) if args.build_dir else os.path.join(base_dir, 'build')

    stamp = os.path.join(install_dir, 'duckdb_odbc_revision.txt')
    stamp_content = f'{sha} {version} {args.build_type} {" ".join(LOADER_COMPONENTS)}\n'
    if not args.source_dir and os.path.isfile(stamp):
        with open(stamp) as f:
            if f.read() == stamp_content:
                print(f'DuckDB {version} ({sha}) is already installed in {install_dir}')
                return

    if not args.source_dir:
        fetch_source(source_dir, repository, sha)

    configure = [
        'cmake',
        '-S',
        source_dir,
        '-B',
        build_dir,
        f'-DCMAKE_BUILD_TYPE={args.build_type}',
        f'-DCMAKE_INSTALL_PREFIX={install_dir}',
        f'-DBUILD_EXTENSIONS={EXTENSIONS}',
        f'-DSTATICALLY_LINK_EXTENSIONS={EXTENSIONS}',
        '-DBUILD_UNITTESTS=OFF',
        '-DBUILD_SHELL=OFF',
        '-DBUILD_BENCHMARKS=OFF',
    ]
    if not args.source_dir:
        # the version the pinned revision was vendored with: a shallow fetch cannot derive it, and extensions are published
        # under the staged version
        configure.append(f'-DOVERRIDE_GIT_DESCRIBE={version}')
    generator = args.generator
    if generator is None and shutil.which('ninja') and not os.path.exists(os.path.join(build_dir, 'CMakeCache.txt')):
        generator = 'Ninja'
    if generator:
        configure += ['-G', generator]
    if args.osx_architectures:
        configure.append(f'-DCMAKE_OSX_ARCHITECTURES={args.osx_architectures}')
    if sys.platform == 'win32':
        configure.append('-DCMAKE_MSVC_RUNTIME_LIBRARY=MultiThreaded$<$<CONFIG:Debug>:Debug>')
    configure += args.cmake_arg
    run(configure)
    run(['cmake', '--build', build_dir, '--config', args.build_type])
    run(['cmake', '--install', build_dir, '--config', args.build_type])

    # The statically linked extensions register through a generated loader, which the driver compiles and calls
    loader_file = os.path.join(install_dir, LOADER_FILE)
    os.makedirs(os.path.dirname(loader_file), exist_ok=True)
    run(
        [
            sys.executable,
            os.path.join(source_dir, 'scripts', 'generate_static_extension_loader.py'),
            '--output',
            loader_file,
        ]
        + LOADER_COMPONENTS
    )

    with open(stamp, 'w') as f:
        f.write(stamp_content)
    print(f'Installed DuckDB {version} ({sha}) into {install_dir}')


if __name__ == '__main__':
    main()
