# -*- coding: utf-8 -*-
import json
import os
import sys

import math
from pymrgeo import MrGeo


def toa_reflectance(image, metadata, band):
    scales = metadata['L1_METADATA_FILE']['RADIOMETRIC_RESCALING']

    mult = scales['REFLECTANCE_MULT_BAND_' + str(band)]
    add = scales['REFLECTANCE_ADD_BAND_' + str(band)]

    print('band: ' + str(band) + ' mult: ' + str(mult) + ' add: ' + str(add))

    rad = ((image * mult) + add)
    return rad

def toa_radiance(image, metadata, band):
    scales = metadata['L1_METADATA_FILE']['RADIOMETRIC_RESCALING']

    mult = scales['RADIANCE_MULT_BAND_' + str(band)]
    add = scales['RADIANCE_ADD_BAND_' + str(band)]

    sun_elev = metadata['L1_METADATA_FILE']['IMAGE_ATTRIBUTES']['SUN_ELEVATION'] * 0.0174533 # degrees to rad

    print('band: ' + str(band) + ' mult: ' + str(mult) + ' add: ' + str(add))

    refl = ((image * mult) + add) / math.sin(sun_elev)
    return refl

if __name__ == "__main__":

    landsat = {}
    # find all the landsat images
    root = '/data/gis-data/images/landsat8/LC80140342014187LGN00'
    if os.path.exists(root):
        for dirname, subdirs, files in os.walk(root):
            for name in files:
                pathname = os.path.join(dirname, name)
                base, ext = os.path.splitext(pathname)
                ext = ext.lower()
                base = base.lower()

                print(name)
                if ext == '.tif':
                    # find the band number
                    split = base.split('_')
                    if len(split) != 2:
                        raise Exception('Bad TIF filename: ' + pathname)

                    print(split[1])
                    b = split[1]
                    if b == 'bqa':
                        landsat[b] = pathname
                    else:
                        # strip off the "B"
                        band = int(b[1:])
                        landsat[band] = pathname
                    pass
                elif ext == '.json':
                    landsat[ext[1:]] = pathname
                    pass

    if not landsat.has_key('json'):
        raise Exception('No JSON metadata file in ' + root)

    with open(landsat['json']) as metafile:
        metadata = json.load(metafile)

    mrgeo = MrGeo()
    # mrgeo.usedebug()

    mrgeo.start()

    red = mrgeo.ingest_image(landsat[4])
    green = mrgeo.ingest_image(landsat[3])
    blue = mrgeo.ingest_image(landsat[2])

    rgb = red.bandcombine(green, blue)
    rgb.export('/data/export/landsat-rgb-dn.tif', singleFile=True)

    r_rad = toa_radiance(red, metadata, 4)
    g_rad = toa_radiance(green, metadata, 3)
    b_rad = toa_radiance(blue, metadata, 2)

    rgb = r_rad.bandcombine(g_rad, b_rad)
    rgb.export('/data/export/landsat-rgb-rad.tif', singleFile=True)

    # compute Top of Atmosphere (TOA) Reflectance of the bands (http://landsat.usgs.gov/Landsat8_Using_Product.php)
    r_refl = toa_reflectance(red, metadata, 4)
    g_refl = toa_reflectance(green, metadata, 3)
    b_refl = toa_reflectance(blue, metadata, 2)

    # combine the bands
    rgb = r_refl.bandcombine(g_refl, b_refl)

    # rgb = rgb.convert('byte', 'fit')

    rgb.export('/data/export/landsat-rgb-refl.tif', singleFile=True)


    # ones = mrgeo.load_image("all-ones-save")

    # slope = ones.slope()

    # hundreds = mrgeo.load_image("all-hundreds")
    # aspect = hundreds.aspect()

    # slope.save("slope-test")
    # aspect.save("aspect-test")

    # print("***** Starting *****")
    # elevation = mrgeo.load_image("small-elevation")
    # elevation.export("/data/export/small-elevation", singleFile=True)

    # slope = small_elevation.slope()
    # slope.save("slope-test")
    # print("***** Finished Slope 1 *****")
    #
    # slope = small_elevation.slope("rad")
    # slope.save("slope-test2")
    # print("***** Finished Slope 2 *****")

    # sub1 = small_elevation - 5
    # sub2 = 5 - small_elevation
    #
    # sub3 = small_elevation.clone()
    # sub3 -= 5

    # hundreds = mrgeo.load_image("all-hundreds-save")
    # hundreds.export("/data/export/100-export-test", singleFile=True)
    #
    # slope = mrgeo.load_image("santiago-aster")
    # slope.export("/data/export/santiago-aster", singleFile=True)

    # hundreds.export("/data/export/hundreds-export-test", singleFile=True)

    # sub = hundreds + ones
    #
    # sub.export("/data/export/101-export-test", singleFile=True)


    # zen = 30.0 * 0.0174532925  # sun 30 deg above the horizon
    # sunaz = 270.0 * 0.0174532925 # sun from 270 deg (west)
    #
    # coszen = math.cos(zen)
    # sinzen = math.sin(zen)
    #
    # slope = elevation.slope()
    # aspect = elevation.aspect()
    #
    # hill = 255 * ((coszen * slope.cos()) + (sinzen * slope.sin() * (sunaz - aspect).cos()))
    # # "hill = 255.0 * ((coszen * cos(sl)) + (sinzen * sin(sl) * cos(sunaz - as)))"
    #
    # hill.export("/data/export/hillshade-test", singleFile=True)


    mrgeo.stop()

    print("***** Done *****")
