#!/bin/bash
set +x
# docker run --network=host --name build_docker_os6 -it -v`pwd`:/root/mnt ghcr.io/4paradigm/centos6_gcc7_hybridsql bash

# self build or workflow

# cd /root/mnt
# tool setup
yum install -y bison bison-devel byacc cppunit-devel patch devtoolset-8-gcc devtoolset-8-gcc-c++
source /opt/rh/devtoolset-8/enable
echo "ID=centos" > /etc/os-release
curl -SLo bazel https://github.com/sub-mod/bazel-builds/releases/download/1.0.0/bazel-1.0.0
# if download slow
# curl -SLo bazel https://openmldb.ai/download/legacy/bazel-1.0.0
chmod +x bazel
export PATH=$PATH:`pwd`
# git clone https://github.com/4paradigm/OpenMLDB.git
# cd OpenMLDB
echo "add patch in fetch cmake"
# skip -lrt in rocksdb
sed -i'' '34s/$/ -DWITH_CORE_TOOLS=OFF/' third-party/cmake/FetchRocksDB.cmake
echo  "modify in .deps needs a make first, download zetasql first(build will fail)"
# sed -i'' '31s/${BUILD_BUNDLED}/ON/' third-party/CMakeLists.txt
timeout 300 make thirdparty BUILD_BUNDLED=ON # ignore error # timeout is better? BUILD_BUNDLED=OFF will download pre-built thirdparty, not good
echo "add patch in zetasql"
sed -i'' "26s/lm'/lm:-lrt'/" .deps/build/src/zetasql/build_zetasql_parser.sh
# skip more target to avoid adding -lrt
sed -i'' '42s/^/#/' .deps/build/src/zetasql/build_zetasql_parser.sh
sed -i'' '6a function realpath () { \n[[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"\n}' .deps/build/src/zetasql/pack_zetasql.sh
# if need faster
# export NPROC=40
# make BUILD_BUNDLED=ON SQL_JAVASDK_ENABLE=ON SQL_PYSDK_ENABLE=ON
# make install

# plus
# if download is slow in zetasql
# sed -i'' '911s#],#,"https://openmldb.ai/download/legacy/icu4c-65_1-src.tgz"],#' .deps/build/src/zetasql/bazel/zetasql_deps_step_2.bzl