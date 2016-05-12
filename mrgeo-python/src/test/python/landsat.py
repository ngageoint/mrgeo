# -*- coding: utf-8 -*-
import json
import os
import sys

import math
from pymrgeo import MrGeo
from pymrgeo.rastermapop import RasterMapOp

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


    print(landsat)

    if not landsat.has_key('json'):
        raise Exception('No JSON metadata file in ' + root)

    with open(landsat['json']) as metafile:
        metadata = json.load(metafile)

    mrgeo = MrGeo()
    mrgeo.usedebug()

    mrgeo.start()

    red = RasterMapOp()
    red.ingest(landsat[4])



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
