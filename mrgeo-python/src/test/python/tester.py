# -*- coding: utf-8 -*-

import sys

import math
from pymrgeo import MrGeo
from pymrgeo.rastermapop import RasterMapOp

if __name__ == "__main__":

    mrgeo = MrGeo()  # forked mrgeo
    #mrgeo = MrGeo(host="localhost", port=12345)  # already running, remote mrgeo

    # sys.exit(1)

    mrgeo.usedebug()

    # images = mrgeo.list_images()

    mrgeo.start()

    ones = mrgeo.load_image("all-ones-save")

    # test error handling
    # foo = 1 / 0
    sl = ones.slope()
    # RasterMapOp.slope(ones, 1)

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
