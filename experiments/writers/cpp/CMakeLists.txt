cmake_minimum_required(VERSION 3.18)

project(ParquetWriterCPP VERSION 1.0 LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------------------------
# Arrow C++
#
# By default Arrow is downloaded and built at configure time, pinned to the commit used for the
# paper, so no Arrow installation is needed. The first configure takes a while (Arrow and its
# bundled dependencies are compiled); later configures reuse the build.
#   -DPARQUET_WRITER_BUILD_ARROW=OFF -DCMAKE_PREFIX_PATH=/path/to/arrow  use an installed Arrow
#   -DARROW_BUILD_ROOT=/some/dir                                          share one Arrow build
#                                                                         between build folders
# ---------------------------------------------------------------------------------------------
option(PARQUET_WRITER_BUILD_ARROW "Download and build Arrow C++ at the pinned commit" ON)

# apache-arrow-26.0.0.dev-10-g68c8cec16d (2026-07-18): the Arrow version used in the paper.
set(ARROW_COMMIT "68c8cec16dec33a87c9976453586c7d51d5fc5c4")
set(ARROW_ARCHIVE_SHA256 "04cca446962588d1b35507b89586c08d386a5f1b298cf65c8cf5986fb7d5b89c")
set(ARROW_BUILD_ROOT "${CMAKE_BINARY_DIR}/arrow" CACHE PATH "Where Arrow is downloaded, built and installed")

if(PARQUET_WRITER_BUILD_ARROW)
    set(arrow_install_dir "${ARROW_BUILD_ROOT}/install")
    set(arrow_stamp "${arrow_install_dir}/.installed-${ARROW_COMMIT}")

    if(NOT EXISTS "${arrow_stamp}")
        set(arrow_archive "${ARROW_BUILD_ROOT}/arrow-${ARROW_COMMIT}.tar.gz")
        set(arrow_source_dir "${ARROW_BUILD_ROOT}/src/arrow-${ARROW_COMMIT}")
        set(arrow_build_dir "${ARROW_BUILD_ROOT}/build")

        if(NOT EXISTS "${arrow_archive}")
            message(STATUS "Downloading Arrow ${ARROW_COMMIT}")
            file(DOWNLOAD "https://github.com/apache/arrow/archive/${ARROW_COMMIT}.tar.gz" "${arrow_archive}"
                 EXPECTED_HASH SHA256=${ARROW_ARCHIVE_SHA256} SHOW_PROGRESS STATUS download_status)
            list(GET download_status 0 download_code)
            if(NOT download_code EQUAL 0)
                file(REMOVE "${arrow_archive}")
                message(FATAL_ERROR "Downloading Arrow failed: ${download_status}")
            endif()
        endif()
        if(NOT EXISTS "${arrow_source_dir}")
            file(ARCHIVE_EXTRACT INPUT "${arrow_archive}" DESTINATION "${ARROW_BUILD_ROOT}/src")
        endif()

        # Same feature set as the Arrow build used in the paper (mimalloc, SSE4.2 + runtime SIMD
        # dispatch, IPC, Parquet). All codecs are enabled so that input files compressed with any of
        # them can be read; this does not change the writer's default properties.
        message(STATUS "Configuring Arrow (log: ${ARROW_BUILD_ROOT}/arrow-*.log)")
        execute_process(
            COMMAND ${CMAKE_COMMAND} -S "${arrow_source_dir}/cpp" -B "${arrow_build_dir}"
                    -G "${CMAKE_GENERATOR}"
                    -DCMAKE_BUILD_TYPE=Release
                    -DCMAKE_INSTALL_PREFIX=${arrow_install_dir}
                    -DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}
                    -DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}
                    -DARROW_BUILD_STATIC=ON
                    -DARROW_BUILD_SHARED=OFF
                    -DARROW_DEPENDENCY_SOURCE=BUNDLED
                    -DARROW_DEPENDENCY_USE_SHARED=OFF
                    -DARROW_PARQUET=ON
                    -DARROW_IPC=ON
                    -DARROW_MIMALLOC=ON
                    -DARROW_JEMALLOC=OFF
                    -DARROW_SIMD_LEVEL=SSE4_2
                    -DARROW_RUNTIME_SIMD_LEVEL=MAX
                    -DARROW_POSITION_INDEPENDENT_CODE=ON
                    -DARROW_WITH_BACKTRACE=ON
                    -DARROW_WITH_ZSTD=ON
                    -DARROW_WITH_ZLIB=ON
                    -DARROW_WITH_SNAPPY=ON
                    -DARROW_WITH_LZ4=ON
                    -DARROW_WITH_BROTLI=ON
                    -DARROW_WITH_BZ2=OFF
                    -DARROW_WITH_UTF8PROC=OFF
                    -DARROW_WITH_RE2=OFF
            OUTPUT_FILE "${ARROW_BUILD_ROOT}/arrow-configure.log"
            ERROR_FILE "${ARROW_BUILD_ROOT}/arrow-configure.log"
            RESULT_VARIABLE arrow_result)
        if(NOT arrow_result EQUAL 0)
            message(FATAL_ERROR "Configuring Arrow failed, see ${ARROW_BUILD_ROOT}/arrow-configure.log")
        endif()

        cmake_host_system_information(RESULT n_cores QUERY NUMBER_OF_LOGICAL_CORES)
        message(STATUS "Building Arrow with ${n_cores} jobs (takes a while the first time)")
        execute_process(
            COMMAND ${CMAKE_COMMAND} --build "${arrow_build_dir}" --parallel ${n_cores}
            OUTPUT_FILE "${ARROW_BUILD_ROOT}/arrow-build.log"
            ERROR_FILE "${ARROW_BUILD_ROOT}/arrow-build.log"
            RESULT_VARIABLE arrow_result)
        if(NOT arrow_result EQUAL 0)
            message(FATAL_ERROR "Building Arrow failed, see ${ARROW_BUILD_ROOT}/arrow-build.log")
        endif()

        execute_process(
            COMMAND ${CMAKE_COMMAND} --install "${arrow_build_dir}"
            OUTPUT_FILE "${ARROW_BUILD_ROOT}/arrow-install.log"
            ERROR_FILE "${ARROW_BUILD_ROOT}/arrow-install.log"
            RESULT_VARIABLE arrow_result)
        if(NOT arrow_result EQUAL 0)
            message(FATAL_ERROR "Installing Arrow failed, see ${ARROW_BUILD_ROOT}/arrow-install.log")
        endif()

        file(TOUCH "${arrow_stamp}")
    endif()

    list(PREPEND CMAKE_PREFIX_PATH "${arrow_install_dir}")
    set(Arrow_DIR "" CACHE PATH "" FORCE)
    set(Parquet_DIR "" CACHE PATH "" FORCE)
endif()

# ---------------------------------------------------------------------------------------------
# SQLite (the run is recorded in a SQLite database). The amalgamation is downloaded and compiled
# into the binary, so the writer stays self-contained.
# ---------------------------------------------------------------------------------------------
set(SQLITE_VERSION_ID "3540100")  # SQLite 3.54.1 (2026-10-16)
set(SQLITE_ARCHIVE_SHA256 "0a9c8e34352dad46006aa82836d5383bff9dc6bc9ce5a60553c05aa30e6ad1f9")
set(sqlite_root "${CMAKE_BINARY_DIR}/sqlite")
set(sqlite_source_dir "${sqlite_root}/sqlite-amalgamation-${SQLITE_VERSION_ID}")
if(NOT EXISTS "${sqlite_source_dir}/sqlite3.c")
    set(sqlite_archive "${sqlite_root}/sqlite-amalgamation-${SQLITE_VERSION_ID}.zip")
    message(STATUS "Downloading SQLite ${SQLITE_VERSION_ID}")
    file(DOWNLOAD "https://www.sqlite.org/2026/sqlite-amalgamation-${SQLITE_VERSION_ID}.zip" "${sqlite_archive}"
         EXPECTED_HASH SHA256=${SQLITE_ARCHIVE_SHA256} STATUS download_status)
    list(GET download_status 0 download_code)
    if(NOT download_code EQUAL 0)
        file(REMOVE "${sqlite_archive}")
        message(FATAL_ERROR "Downloading SQLite failed: ${download_status}")
    endif()
    file(ARCHIVE_EXTRACT INPUT "${sqlite_archive}" DESTINATION "${sqlite_root}")
endif()

add_library(sqlite3 STATIC "${sqlite_source_dir}/sqlite3.c")
target_include_directories(sqlite3 PUBLIC "${sqlite_source_dir}")
# Single-threaded use without loadable extensions: no pthread/dlopen dependencies.
target_compile_definitions(sqlite3 PRIVATE SQLITE_THREADSAFE=0 SQLITE_OMIT_LOAD_EXTENSION)

# ---------------------------------------------------------------------------------------------
# Writer
#
# By default the binary is fully static and self-contained: Arrow, Parquet, their bundled
# dependencies (zstd, ...), the C++ runtime and glibc are all linked in, so it runs on any
# x86-64 Linux machine. This requires the static glibc (libc6-dev on Debian/Ubuntu).
#   -DPARQUET_WRITER_FULLY_STATIC=OFF  keep glibc dynamic (needs glibc >= the build machine's)
#   -DPARQUET_WRITER_STATIC=OFF        link Arrow/Parquet as shared libraries (only with an
#                                      installed Arrow that has shared libraries)
# ---------------------------------------------------------------------------------------------
option(PARQUET_WRITER_STATIC "Link Arrow, Parquet and the C++ runtime statically" ON)
option(PARQUET_WRITER_FULLY_STATIC "Also link glibc statically (requires PARQUET_WRITER_STATIC)" ON)

find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
message(STATUS "Using Arrow ${Arrow_VERSION} from ${Arrow_DIR}")

add_executable(parquet_writer_cpp
    src/parquet_writer.cpp
)
target_link_libraries(parquet_writer_cpp PRIVATE sqlite3)

if(PARQUET_WRITER_STATIC)
    target_link_libraries(parquet_writer_cpp PRIVATE
        Parquet::parquet_static
        Arrow::arrow_static
    )
    if(PARQUET_WRITER_FULLY_STATIC)
        # Arrow references dlopen for optional plugins (e.g. HDFS); the writer only does local
        # file I/O, so the linker warning about dlopen in static binaries does not apply.
        target_link_options(parquet_writer_cpp PRIVATE -static)
    else()
        target_link_options(parquet_writer_cpp PRIVATE -static-libstdc++ -static-libgcc)
    endif()
else()
    target_link_libraries(parquet_writer_cpp PRIVATE
        Parquet::parquet_shared
        Arrow::arrow_shared
    )
endif()
