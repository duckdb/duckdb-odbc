.PHONY: release debug duckdb test clean format tidy tidy

GENERATOR=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

ODBC_CONFIG_FLAG=
ifneq ($(ODBC_CONFIG),)
	ODBC_CONFIG_FLAG=-DODBC_CONFIG=${ODBC_CONFIG}
endif

OSX_BUILD_AMD64_FLAG=
DUCKDB_OSX_FLAG=
ifneq ($(OSX_BUILD_AMD64),)
	OSX_BUILD_AMD64_FLAG=-DOSX_BUILD_AMD64=1
	DUCKDB_OSX_FLAG=--osx-architectures x86_64
endif

# The DuckDB engine the driver links: an install prefix given through DUCKDB_ROOT, or else the revision pinned in
# third_party/duckdb_cpp/duckdb_revision.cmake, built once into build/duckdb.
ifeq ($(DUCKDB_ROOT),)
	DUCKDB_ROOT_DIR=$(CURDIR)/build/duckdb/install
	DUCKDB_STEP=duckdb
else
	DUCKDB_ROOT_DIR=$(DUCKDB_ROOT)
	DUCKDB_STEP=
endif

duckdb:
	python3 scripts/build_duckdb.py --install-dir $(DUCKDB_ROOT_DIR) $(DUCKDB_OSX_FLAG)

release: $(DUCKDB_STEP)
	mkdir -p build/release
	cd build/release && cmake -DCMAKE_BUILD_TYPE=Release -DDUCKDB_ROOT=$(DUCKDB_ROOT_DIR) $(GENERATOR) $(ODBC_CONFIG_FLAG) $(OSX_BUILD_AMD64_FLAG) ../.. && cmake --build . --config Release

debug: $(DUCKDB_STEP)
	mkdir -p build/debug
	cd build/debug && cmake -DCMAKE_BUILD_TYPE=Debug -DDUCKDB_ROOT=$(DUCKDB_ROOT_DIR) $(GENERATOR) $(ODBC_CONFIG_FLAG) $(OSX_BUILD_AMD64_FLAG) ../.. && cmake --build . --config Debug

format-fix:
	rm -rf src/amalgamation/*
	python3 scripts/format.py --all --fix --noconfirm

format-check:
	python3 scripts/format.py --all --check

format-check-silent:
	python3 scripts/format.py --all --check --silent

tidy-check:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 ../.. && \
	python3 ../../scripts/run-clang-tidy.py -quiet ${TIDY_THREAD_PARAMETER} ${TIDY_BINARY_PARAMETER}

tidy-fix:
	mkdir -p ./build/tidy && \
	cd build/tidy && \
	cmake -DCLANG_TIDY=1 ../.. && \
	python3 ../../scripts/run-clang-tidy.py -fix

clean:
	rm -rf build