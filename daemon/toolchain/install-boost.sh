#!/usr/bin/env bash -e

if [ -z "${BOOST_VERSION}" ]; then
    echo "Need to set BOOST_VERSION"
    exit 1
fi

if [ -z "${BOOST_INSTALL_DIR}" ]; then
    echo "Need to set BOOST_INSTALL_DIR"
    exit 1
fi

BOOST_LIBS="test,chrono,coroutine,program_options,random,regex,system,thread"
BOOST_VERSION_UNDERSCORES="$(echo "${BOOST_VERSION}" | sed 's/\./_/g')"
BOOST_TARBALL="boost_${BOOST_VERSION_UNDERSCORES}.tar.gz"
BOOST_URL="http://sourceforge.net/projects/boost/files/boost/${BOOST_VERSION}/${BOOST_TARBALL}/download"
BOOST_PACKAGE_DIR=${BOOST_INSTALL_DIR}/${BOOST_VERSION_UNDERSCORES}

if [ ! -e ${BOOST_PACKAGE_DIR}/libs ]; then
    echo "Boost not found in the cache, get and extract it..."
    mkdir -p ${BOOST_PACKAGE_DIR}
    curl -L ${BOOST_URL} | tar -xj -C ${BOOST_PACKAGE_DIR} --strip-components=1
    cd ${BOOST_PACKAGE_DIR}
    ./bootstrap.sh --prefix=${BOOST_PACKAGE_DIR} --with-libraries=${BOOST_LIBS}
    ./b2 install -d0
else
    echo "Using cached Boost"
fi