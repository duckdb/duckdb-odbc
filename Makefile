.PHONY: release debug test clean format tidy tidy

GENERATOR=
ifeq ($(GEN),ninja)
	GENERATOR=-G "Ninja"
	FORCE_COLOR=-DFORCE_COLORED_OUTPUT=1
endif

ODBC_CONFIG_FLAG=
ifneq ($(ODBC_CONFIG),)
	ODBC_CONFIG_FLAG=-DODBC_CONFIG=${ODBC_CONFIG}
endif

OSX_BUILD_UNIVERSAL_FLAG=
ifneq ($(OSX_BUILD_UNIVERSAL),)
	OSX_BUILD_UNIVERSAL_FLAG=-DOSX_BUILD_UNIVERSAL=1
endif

release:
	mkdir -p build/release
	cd build/release && cmake -DCMAKE_BUILD_TYPE=Release $(GENERATOR) $(ODBC_CONFIG_FLAG) $(OSX_BUILD_UNIVERSAL_FLAG) ../.. && cmake --build . --config Release

debug:
	mkdir -p build/debug
	cd build/debug && cmake -DCMAKE_BUILD_TYPE=Debug $(GENERATOR) $(ODBC_CONFIG_FLAG) $(OSX_BUILD_UNIVERSAL_FLAG) ../.. && cmake --build . --config Debug

format-fix:
	rm -rf src/amalgamation/*
	python3 scripts/format.py --all --fix --noconfirm

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
	cmake -DCLANG_TIDY=1 -DDISABLE_UNITY=1 -DBUILD_EXTENSIONS=parquet -DBUILD_SHELL=0 ../.. && \
	python3 ../../scripts/run-clang-tidy.py -fix


clean:
	rm -rf build