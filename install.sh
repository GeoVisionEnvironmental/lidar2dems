#!/bin/bash

cd "$(dirname "$0")"
CPLUS_INCLUDE_PATH=/usr/include/gdal C_INCLUDE_PATH=/usr/include/gdal ./setup.py install --prefix="$1"
