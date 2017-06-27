#!/usr/bin/env python
################################################################################
#   lidar2dems - utilties for creating DEMs from LiDAR data
#
#   AUTHOR: Matthew Hanson, matt.a.hanson@gmail.com
#
#   Copyright (C) 2015 Applied Geosolutions LLC, oss@appliedgeosolutions.com
#
#   Redistribution and use in source and binary forms, with or without
#   modification, are permitted provided that the following conditions are met:
#
#   * Redistributions of source code must retain the above copyright notice, this
#     list of conditions and the following disclaimer.
#
#   * Redistributions in binary form must reproduce the above copyright notice,
#     this list of conditions and the following disclaimer in the documentation
#     and/or other materials provided with the distribution.
#
#   THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
#   AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
#   IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
#   DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
#   FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
#   DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
#   SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
#   CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
#   OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
#   OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
################################################################################

# Library functions for creating DEMs from Lidar data

import os
import json as jsonlib
import tempfile

from shapely.wkt import loads
import glob
from datetime import datetime
import uuid
from .utils import class_params, class_suffix, dem_products, gap_fill


""" JSON Functions """


def _json_base():
    """ Create initial JSON for PDAL pipeline """
    return {'pipeline': []}


def _json_gdal_base(fout, output, radius, resolution=1, site=None):
    """ Create initial JSON for PDAL pipeline containing a Writer element """
    json = _json_base()

    if len(output) > 1:
        # TODO: we might want to create a multiband raster with max/min/idw
        # in the future
        print "More than 1 output, will only create {0}".format(output[0])
        output = [output[0]]

    json['pipeline'].insert(0, {
        'type': 'writers.gdal',
        'resolution': resolution,
        'radius': radius,
        'filename': '{0}.{1}.tif'.format(fout, output[0]),
        'output_type': output[0]
    })

    return json


def _json_las_base(fout):
    """ Create initial JSON for writing to a LAS file """
    json = _json_base()
    json['pipeline'].insert(0, {
        'type': 'writers.las',
        'filename': fout  
    })
    return json


def _json_add_decimation_filter(json, step):
    """ Add decimation Filter element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.decimation',
            'step': step
        })
    return json


def _json_add_classification_filter(json, classification, equality="equals"):
    """ Add classification Filter element and return """
    limits = 'Classification[{0}:{0}]'.format(classification)
    if equality == 'max':
        limits = 'Classification[:{0}]'.format(classification)

    json['pipeline'].insert(0, {
            'type': 'filters.range',
            'limits': limits
        })
    return json


def _json_add_maxsd_filter(json, meank=20, thresh=3.0):
    """ Add outlier Filter element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.outlier',
            'method': 'statistical',
            'mean_k': meank,
            'multiplier': thresh
        })
    return json


def _json_add_maxz_filter(json, maxz):
    """ Add max elevation Filter element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.range',
            'limits': 'Z[:{0}]'.format(maxz)
        })

    return json


def _json_add_maxangle_filter(json, maxabsangle):
    """ Add scan angle Filter element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.range',
            'limits': 'ScanAngleRank[{0}:{1}]'.format(str(-float(maxabsangle)), maxabsangle)
        })
    return json


def _json_add_scanedge_filter(json, value):
    """ Add EdgeOfFlightLine Filter element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.range',
            'limits': 'EdgeOfFlightLine[{0}:{0}]'.format(value)
        })
    return json


def _json_add_returnnum_filter(json, value):
    """ Add ReturnNum Filter element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.range',
            'limits': 'ReturnNum[{0}:{0}]'.format(value)
        })
    return json


def _json_add_filters(json, maxsd=None, maxz=None, maxangle=None, returnnum=None):
    if maxsd is not None:
        json = _json_add_maxsd_filter(json, thresh=maxsd)
    if maxz is not None:
        json = _json_add_maxz_filter(json, maxz)
    if maxangle is not None:
        json = _json_add_maxangle_filter(json, maxangle)
    if returnnum is not None:
        json = _json_add_returnnum_filter(json, returnnum)
    return json


def _json_add_crop_filter(json, wkt):
    """ Add cropping polygon as Filter Element and return """
    json['pipeline'].insert(0, {
            'type': 'filters.crop',
            'polygon': wkt
        })
    return json


def _json_add_reader(json, filename):
    """ Add LAS Reader Element and return """
    json['pipeline'].insert(0, {
            'type': 'readers.las',
            'filename': os.path.abspath(filename)
        })
    return json


def _json_add_readers(json, filenames):
    """ Add merge Filter element and readers to a Writer element and return Filter element """
    for f in filenames:
        _json_add_reader(json, f)

    if len(filenames) > 1:
        json['pipeline'].insert(0, {
                'type': 'filters.merge'
            })

    return json


def _json_print(json):
    """ Pretty print JSON """
    print jsonlib.dumps(json, indent=4, separators=(',', ': '))


""" Run PDAL commands """

def run_pipeline(json, verbose=False):
    """ Run PDAL Pipeline with provided JSON """
    if verbose:
        _json_print(json)

    # write to temp file
    f, jsonfile = tempfile.mkstemp(suffix='.json')
    if verbose:
        print 'Pipeline file: %s' % jsonfile
    os.write(f, jsonlib.dumps(json))
    os.close(f)

    cmd = [
        'pdal',
        'pipeline',
        '-i %s' % jsonfile
    ]
    if verbose:
        out = os.system(' '.join(cmd))
    else:
        out = os.system(' '.join(cmd) + ' > /dev/null 2>&1')
    os.remove(jsonfile)


def run_pdalground(fin, fout, slope, cellsize, maxWindowSize, maxDistance, approximate=False, verbose=False):
    """ Run PDAL ground """
    cmd = [
        'pdal',
        'ground',
        '-i %s' % fin,
        '-o %s' % fout,
        '--slope %s' % slope,
        '--cell_size %s' % cellsize
    ]
    if maxWindowSize is not None:
        cmd.append('--max_window_size %s' %maxWindowSize)
    if maxDistance is not None:
        cmd.append('--max_distance %s' %maxDistance)

    if approximate:
        cmd.append('--approximate')

    if verbose:
        cmd.append('--developer-debug')
        print ' '.join(cmd)
    print ' '.join(cmd)
    out = os.system(' '.join(cmd))
    if verbose:
        print out


# LiDAR Classification and DEM creation

def merge_files(filenames, fout=None, site=None, buff=20, decimation=None, verbose=False):
    """ Create merged las file """
    start = datetime.now()
    if fout is None:
        # TODO ? use temp folder?
        fout = os.path.join(os.path.abspath(os.path.dirname(filenames[0])), str(uuid.uuid4()) + '.las')
    json = _json_las_base(fout)

    if decimation is not None:
        json = _json_add_decimation_filter(json, decimation)
    # need to build PDAL with GEOS
    if site is not None:
        wkt = loads(site.WKT()).buffer(buff).wkt
        json = _json_add_crop_filter(json, wkt)
    _json_add_readers(json, filenames)
    try:
        run_pipeline(json, verbose=verbose)
    except:
        raise Exception("Error merging LAS files")
    print 'Created merged file %s in %s' % (os.path.relpath(fout), datetime.now() - start)
    return fout


def classify(filenames, fout, slope=None, cellsize=None, maxWindowSize=10, maxDistance=1,
             site=None, buff=20, decimation=None, approximate=False, verbose=False):
    """ Classify files and output single las file """
    start = datetime.now()

    print 'Classifying %s files into %s' % (len(filenames), os.path.relpath(fout))

    # problem using PMF in JSON - instead merge to ftmp and run 'pdal ground'
    ftmp = merge_files(filenames, site=site, buff=buff, decimation=decimation, verbose=verbose)

    try:
        run_pdalground(ftmp, fout, slope, cellsize, maxWindowSize, maxDistance, approximate=approximate, verbose=verbose)
        # verify existence of fout
        if not os.path.exists(fout):
            raise Exception("Error creating classified file %s" % fout)
    except:
        raise Exception("Error creating classified file %s" % fout)
    finally:
        # remove temp file
        os.remove(ftmp)

    print 'Created %s in %s' % (os.path.relpath(fout), datetime.now() - start)
    return fout


def create_dems(filenames, demtype, radius=['0.56'], site=None, gapfill=False,
                outdir='', suffix='', overwrite=False, resolution=0.1, **kwargs):
    """ Create DEMS for multiple radii, and optionally gapfill """
    fouts = []
    for rad in radius:
        fouts.append(
            create_dem(filenames, demtype,
                       radius=rad, site=site, outdir=outdir, suffix=suffix, overwrite=overwrite, resolution=resolution, **kwargs))
    fnames = {}
    # convert from list of dicts, to dict of lists
    for product in fouts[0].keys():
        fnames[product] = [f[product] for f in fouts]
    fouts = fnames

    # gapfill all products (except density)
    _fouts = {}
    if gapfill:
        for product in fouts.keys():
            # do not gapfill, but keep product pointing to first radius run
            if product == 'den':
                _fouts[product] = fouts[product][0]
                continue
            # output filename
            bname = '' if site is None else site.Basename() + '_'
            fout = os.path.join(outdir, bname + '%s%s.%s.tif' % (demtype, suffix, product))
            if not os.path.exists(fout) or overwrite:
                gap_fill(fouts[product], fout, site=site)
            _fouts[product] = fout
    else:
        # only return single filename (first radius run)
        for product in fouts.keys():
            _fouts[product] = fouts[product][0]

    return _fouts


def create_dem(filenames, demtype, radius='0.56', site=None, decimation=None,
               maxsd=None, maxz=None, maxangle=None, returnnum=None,
               products=None, outdir='', suffix='', overwrite=False, verbose=False, resolution=0.1):
    """ Create DEM from collection of LAS files """
    start = datetime.now()
    # filename based on demtype, radius, and optional suffix
    bname = '' if site is None else site.Basename() + '_'
    bname = os.path.join(os.path.abspath(outdir), '%s%s_r%s%s' % (bname, demtype, radius, suffix))
    ext = 'tif'

    # products (den, max, etc)
    if products is None:
        products = dem_products(demtype)
    fouts = {o: bname + '.%s.%s' % (o, ext) for o in products}
    prettyname = os.path.relpath(bname) + ' [%s]' % (' '.join(products))

    # run if any products missing (any extension version is ok, i.e. vrt or tif)
    run = False
    for f in fouts.values():
        if len(glob.glob(f[:-3] + '*')) == 0:
            run = True

    if run or overwrite:
        print 'Creating %s from %s files' % (prettyname, len(filenames))
        # JSON pipeline
        json = _json_gdal_base(bname, products, radius, resolution)

        if decimation is not None:
            json = _json_add_decimation_filter(json, decimation)

        json = _json_add_filters(json, maxsd, maxz, maxangle, returnnum)
        
        if demtype == 'dsm':
            json = _json_add_classification_filter(json, 2, equality='max')
        elif demtype == 'dtm':
            json = _json_add_classification_filter(json, 2)

        _json_add_readers(json, filenames)

        run_pipeline(json, verbose=verbose)
        # verify existence of fout
        exists = True
        for f in fouts.values():
            if not os.path.exists(f):
                exists = False
        if not exists:
            raise Exception("Error creating dems: %s" % ' '.join(fouts))

    print 'Completed %s in %s' % (prettyname, datetime.now() - start)
    return fouts
