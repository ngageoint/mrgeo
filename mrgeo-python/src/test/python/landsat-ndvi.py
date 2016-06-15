# -*- coding: utf-8 -*-
import gdal
import getopt
import json
import numpy
import os
from gdalconst import *
from osgeo import osr
import sys
import tempfile

import math
from dateutil.parser import parse
from pymrgeo import MrGeo
from pymrgeo.rastermapop import RasterMapOp


# The purpose of this script is to provide an example of how pymrgeo can
# be used for ingesting and processing imagery. It makes use of Landsat8
# source imagery which is freely available in s3://landsat-pds/L8/
#
# This script will produce a single MrGeo image with the normalized difference
# vegetation index (NDVI) computed across a set of source scenes from Landsat8.
# The user should copy the Landsat data for all of the scenes they wish to
# process into a local directory structure. It processes bands 4, 5 and BQA
# and reads the *_MTL.json files for metadata.
#
# The user specifies the root directory for the images to process along with
# the month and year for which to compute the NDVI. The script finds the latest
# timestamp for each scene within that month and year, then uses GDAL to
# pre-process the source file to compute the TOA reflectance for bands 4 (red)
# and 5 (nir) and saves the results to temp files. It then ingests all of those
# temp files as a single MrGeo image, and using pymrgeo map algebra capabilities,
# computes the NDVI and saves it as a MrGeo image.

def toa_reflectance(Array, bqa_data, metadata, band):
    scales = metadata['L1_METADATA_FILE']['RADIOMETRIC_RESCALING']

    mult = scales['REFLECTANCE_MULT_BAND_' + str(band)]
    add = scales['REFLECTANCE_ADD_BAND_' + str(band)]

    print('band: ' + str(band) + ' mult: ' + str(mult) + ' add: ' + str(add))

    # Ignore pixels with a high certainty of cloud cover, ice/snow, or water.
    Array[(numpy.bitwise_and(bqa_data, 0x8820) != 0)] = numpy.nan
    conds = numpy.logical_not(numpy.isnan(Array))
    Array[conds] *= mult
    Array[conds] += add

def toa_radiance(Array, metadata, band):
    scales = metadata['L1_METADATA_FILE']['RADIOMETRIC_RESCALING']

    mult = scales['RADIANCE_MULT_BAND_' + str(band)]
    add = scales['RADIANCE_ADD_BAND_' + str(band)]

    sun_elev = metadata['L1_METADATA_FILE']['IMAGE_ATTRIBUTES']['SUN_ELEVATION'] * 0.0174533 # degrees to rad

    print('band: ' + str(band) + ' mult: ' + str(mult) + ' add: ' + str(add))

    Array[Array != 0] = ((Array * mult) + add) / math.sin(sun_elev)

def load_all_metadata(root):
    metadata = []
    # find all the landsat images
    if os.path.exists(root):
        for dirname, subdirs, files in os.walk(root):
            for name in files:
                pathname = os.path.join(dirname, name)
                base, ext = os.path.splitext(pathname)
                ext = ext.lower()
                base = base.lower()

                if ext == '.json':
                    with open(pathname) as metafile:
                        md = json.load(metafile)
                        md["PARENT_DIR"] = dirname
                        md["BQA"] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_QUALITY"])
                        md[1] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_1"])
                        md[2] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_2"])
                        md[3] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_3"])
                        md[4] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_4"])
                        md[5] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_5"])
                        md[6] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_6"])
                        md[7] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_7"])
                        md[8] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_8"])
                        md[9] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_9"])
                        md[10] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_10"])
                        md[11] = os.path.join(dirname, md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["FILE_NAME_BAND_11"])
                        metadata.append(md)
                    metafile.close()
                    pass
    return metadata

# Function to read the original file's projection:
def GetGeoInfo(FileName):
    SourceDS = gdal.Open(FileName, GA_ReadOnly)
    NDV = SourceDS.GetRasterBand(1).GetNoDataValue()
    xsize = SourceDS.RasterXSize
    ysize = SourceDS.RasterYSize
    GeoT = SourceDS.GetGeoTransform()
    Projection = osr.SpatialReference()
    Projection.ImportFromWkt(SourceDS.GetProjectionRef())
    data_type = SourceDS.GetRasterBand(1).DataType
    data_type = gdal.GetDataTypeName(data_type)
    return NDV, xsize, ysize, GeoT, Projection, data_type

# Function to write a new file.
def CreateGeoTiff(Name, Array, driver, NDV,
                  xsize, ysize, GeoT, Projection, data_type):
    if data_type == 'Float32':
        data_type = gdal.GDT_Float32
    # Set up the dataset
    DataSet = driver.Create( Name, xsize, ysize, 1, data_type )
    # the '1' is for band 1.
    DataSet.SetGeoTransform(GeoT)
    DataSet.SetProjection( Projection.ExportToWkt() )
    # Write the array
    DataSet.GetRasterBand(1).WriteArray( Array )
    if NDV is not None:
        DataSet.GetRasterBand(1).SetNoDataValue(NDV)
    # DataSet.close()

def main(argv):
    root = ''
    month = -1
    year = -1
    output_postfix = ''
    try:
        opts, args = getopt.getopt(argv,"hr:m:y:o:",["root=","month=,year=,output-postfix="])
    except getopt.GetoptError:
        print 'multi-landsat.py -r <rootdir> -m <monthnum> -y <year> -o <output-postfix>'
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print 'multi-landsat.py -r <rootdir> -m <monthnum> -y <year> -o <output-postfix>'
            sys.exit()
        elif opt in ("-r", "--root"):
            root = arg
        elif opt in ("-m", "--month"):
            month = int(arg)
        elif opt in ("-y", "--year"):
            year = int(arg)
        elif opt in ("-o", "--output-postfix"):
            output_postfix = arg

    print 'Root is %s' % (root)
    print 'Month is %d' % (month)
    print 'Year is %d' % (year)

    metadata = load_all_metadata(root)

    # Filter the metadata to find the most recent imagery for each scene.
    # It is assumed that the root directory only contains scenes we are
    # interested in.
    filtered_metadata = {}
    for md in metadata:
        acqdate = parse(md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["DATE_ACQUIRED"])
        if acqdate.month == month and acqdate.year == year:
            wrs_path = int(md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["WRS_PATH"])
            wrs_row = int(md["L1_METADATA_FILE"]["PRODUCT_METADATA"]["WRS_ROW"])
            key = "%03d%03d" % (wrs_path, wrs_row)
            if filtered_metadata.get(key) is None:
                filtered_metadata[key] = md
            else:
                existing_acq_date = parse(filtered_metadata[key]["L1_METADATA_FILE"]["PRODUCT_METADATA"]["DATE_ACQUIRED"])
                if acqdate > existing_acq_date:
                    filtered_metadata[key] = md
    for key in filtered_metadata:
        print "Processing scene at path %s, row %s, acquired %s, at %s" % (filtered_metadata[key]["L1_METADATA_FILE"]["PRODUCT_METADATA"]["WRS_PATH"],
                                                                    filtered_metadata[key]["L1_METADATA_FILE"]["PRODUCT_METADATA"]["WRS_ROW"],
                                                                    filtered_metadata[key]["L1_METADATA_FILE"]["PRODUCT_METADATA"]["DATE_ACQUIRED"],
                                                                    filtered_metadata[key]["PARENT_DIR"])

    # being a separate image
    mrgeo = MrGeo()
    mrgeo.usedebug()

    mrgeo.start()

    # Let's go ahead and ingest those images into MrGeo, with each band
    red_refl = ingest_reflectance_image(mrgeo, filtered_metadata, 4, "landsat-red-refl" + output_postfix)
    nir_refl = ingest_reflectance_image(mrgeo, filtered_metadata, 5, "landsat-nir-refl" + output_postfix)

    ndvi = (nir_refl - red_refl) / (nir_refl + nir_refl)
    ndvi.save('landsat-ndvi' + output_postfix)

    mrgeo.stop()
    print("***** Done *****")

def ingest_reflectance_image(mrgeo, filtered_metadata, band, image_name):
    cnt = 0
    red_refl_paths = mrgeo.gateway.new_array(mrgeo.gateway.jvm.String, len(filtered_metadata))
    driver = gdal.GetDriverByName('GTiff')
    for key in filtered_metadata:
        md = filtered_metadata[key]
        red_ds = gdal.Open(md[band], GA_ReadOnly)
        bqa_ds = gdal.Open(md["BQA"], GA_ReadOnly)
        ndv, xsize, ysize, GeoT, Projection, data_type = GetGeoInfo(md[band])
        if ndv is None:
            ndv = 0
        red_band = red_ds.GetRasterBand(1)
        red_data = red_band.ReadAsArray()
        bqa_band = bqa_ds.GetRasterBand(1)
        bqa_data = bqa_band.ReadAsArray()
        data_type = gdal.GDT_Float32
        red_data = numpy.float32(red_data)
        # Change nodata value to NaN since we're changing the type to Float32
        red_data[red_data == ndv] = RasterMapOp.nan()
        ndv = RasterMapOp.nan()
        toa_reflectance(red_data, bqa_data, md, band)
        # Now turn the array into a GTiff.
        ntf = tempfile.NamedTemporaryFile(suffix=".tif", delete=False)
        red_temp_file = ntf.name
        CreateGeoTiff(red_temp_file, red_data, driver, ndv,
                      xsize, ysize, GeoT, Projection, data_type)
        red_refl_paths[cnt] = red_temp_file
        cnt += 1
        # close the datasets
        red_ds = None
        bqa_ds = None

    red = mrgeo.ingest_image(red_refl_paths)
    red.save(image_name)
    for rf in red_refl_paths:
        os.remove(rf)
    return red

if __name__ == "__main__":
    main(sys.argv[1:])
