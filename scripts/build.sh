#! /bin/bash

cd $(dirname $0)/..

# Ensure boost directory exists and download if needed
echo "Checking boost dependency..."
mkdir -p boost
if [ ! -f "boost/boost_1_77_0.tar.bz2" ]; then
    echo "Downloading boost 1.77.0..."
    cd boost
    wget -q https://sourceforge.net/projects/boost/files/boost/1.77.0/boost_1_77_0.tar.bz2
    tar xfj boost_1_77_0.tar.bz2
    cd ..
fi

# Prepare build directory with clean structure
echo "Setting up build directory structure..."
mkdir -p build/data
mkdir -p build/proxy
mkdir -p build/server

# Build DuckDB (third_party submodule) if its shared library is absent.
# Artifacts stay inside the submodule and survive rebuilds of build/.
if [ ! -f "third_party/duckdb/build/release/src/libduckdb.so" ]; then
    echo "Building DuckDB (release)..."
    (cd third_party/duckdb && GEN=ninja EXTRA_CMAKE_VARIABLES="-DBUILD_UNITTESTS=FALSE" make release)
fi

# Build configuration
SERVER_BUILD_TYPE=${SERVER_BUILD_TYPE:-Release}
MYSQL_BUILD_TYPE=${MYSQL_BUILD_TYPE:-Release}

# Create proxy copy with necessary dependencies in build directory
echo "Creating proxy build structure..."
cp -r proxy build/
rm -rf build/proxy/proto
mkdir -p build/proxy/proto
cp -a proto/. build/proxy/proto/

# Create MySQL storage engine link to build directory version
ln -sf $(pwd)/build/proxy third_party/mysql-server/storage/helios

# Build MySQL with proxy storage engine  
echo "Building MySQL with proxy (CMAKE_BUILD_TYPE=${MYSQL_BUILD_TYPE})..."
cd build

cmake ../third_party/mysql-server \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=1 \
    -DWITH_BUILD_ID=0 \
    -DWITH_ASAN=0 \
    -DCMAKE_BUILD_TYPE=${MYSQL_BUILD_TYPE} \
    -DDOWNLOAD_BOOST=0 \
    -DWITH_BOOST=../boost/boost_1_77_0 \
    -DWITHOUT_EXAMPLE_STORAGE_ENGINE=1 \
    -DWITHOUT_FEDERATED_STORAGE_ENGINE=1 \
    -DWITHOUT_ARCHIVE_STORAGE_ENGINE=1 \
    -DWITHOUT_BLACKHOLE_STORAGE_ENGINE=0 \
    -DWITHOUT_NDB_STORAGE_ENGINE=1 \
    -DWITHOUT_NDBCLUSTER_STORAGE_ENGINE=1 \
    -DWITHOUT_PARTITION_STORAGE_ENGINE=1 \
    -G Ninja

ninja $1 -j `nproc`

cd ..

# Build server. It links the MySQL charset archives, which the MySQL build
# above produces.
echo "Building server (CMAKE_BUILD_TYPE=${SERVER_BUILD_TYPE})..."
cd build/server
cmake ../../server \
    -DCMAKE_BUILD_TYPE=${SERVER_BUILD_TYPE} \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=1
make -j `nproc`
cd ../..
