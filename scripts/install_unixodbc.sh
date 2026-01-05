#!/bin/bash
set -x
rm -rf build/unixodbc
mkdir -p build/unixodbc
cd build/unixodbc
mkdir sources build
curl -O -L http://www.unixodbc.org/unixODBC-2.3.11.tar.gz || curl -O -L http://github.com/lurcher/unixODBC/releases/download/2.3.11/unixODBC-2.3.11.tar.gz
tar -xf unixODBC-2.3.11.tar.gz -C sources --strip-components 1
cd sources
./configure --prefix `cd ../build; pwd` --disable-debug --disable-dependency-tracking --enable-static --disable-shared --enable-gui=no --enable-inicaching=no $@
make -j install