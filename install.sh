#!/bin/bash

cd "$(dirname "$0")"
CPLUS_INCLUDE_PATH=/usr/include/gdal C_INCLUDE_PATH=/usr/include/gdal ./setup.py install
if [ ! -f /usr/bin/pdal ]; then
	ln -vs /code/SuperBuild/build/pdal/bin/pdal /usr/bin/pdal
fi
