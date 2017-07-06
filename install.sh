#!/bin/bash

cd "$(dirname "$0")"
python_dist_dir="$1/lib/python2.7/dist-packages/"
if [ ! -e $python_dist_dir ]; then
	echo "Python dist-packages dir does not exist. Creating it now..."
	mkdir -p $python_dist_dir
fi
CPLUS_INCLUDE_PATH=/usr/include/gdal C_INCLUDE_PATH=/usr/include/gdal ./setup.py install --prefix="$1"  --install-layout deb
