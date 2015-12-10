import sys

from pymrgeo import MrGeo
from pymrgeo.rastermapop import RasterMapOp

if __name__ == "__main__":
    mrgeo = MrGeo()

    mrgeo.usedebug()

    mrgeo.start()

    # ones = mrgeo.load_resource("all-ones")

    # slope = ones.slope()

    # hundreds = mrgeo.load_resource("all-hundreds")
    # aspect = hundreds.aspect()

    # slope.save("slope-test")
    # aspect.save("aspect-test")

    print("***** Starting *****")
    small_elevation = mrgeo.load_resource("small-elevation")
    # slope = small_elevation.slope()
    # slope.save("slope-test")
    # print("***** Finished Slope 1 *****")
    #
    # slope = small_elevation.slope("rad")
    # slope.save("slope-test2")
    # print("***** Finished Slope 2 *****")

    sub1 = small_elevation - 5
    sub2 = 5 - small_elevation

    sub3 = small_elevation.clone()
    sub3 -= 5

    mrgeo.stop()

    print("***** Done *****")