from py4j.java_gateway import java_import
from pymrgeo.rastermapop import RasterMapOp
from unittest import TestCase
from functools import partial

class RasterMapOpTestSupport(TestCase):

    def __init__(self, mrgeo):
        self._mrgeo = mrgeo
        jvm = self._mrgeo._get_jvm()
        # Import the raster map op test support class and all other needed classes
        java_import(jvm, "org.mrgeo.core.*")
        java_import(jvm, "org.mrgeo.data.*")
        java_import(jvm, "org.mrgeo.data.raster.RasterWritable")
        java_import(jvm, "org.mrgeo.mapalgebra.utils.StandaloneRasterMapOpTestSupport")
        java_import(jvm, "org.mrgeo.mapalgebra.raster.RasterMapOp")
        java_import(jvm, "org.apache.spark.rdd.RDD")
        java_import(jvm, "org.mrgeo.utils.tms.*")
        self._rasterMapOpTestSupport = jvm.StandaloneRasterMapOpTestSupport()
        self._jvm = jvm
        self._sparkContext = mrgeo.sparkContext
        self._rasterMapOpTestSupport.useSparkContext(self._sparkContext)


    # Default image no data
        self._defaultImageNoData = self._getDoubleArray([0.0])
        # Default image initial data
        self._defaultImageInitialData = self._getDoubleArray([1.0])


    def createRasterMapOp(self, tileIds, zoomLevel, tileSize, name='', imageNoData=None, imageInitialData=None):
        if imageNoData is None:
            imageNoData = self._defaultImageNoData
        if imageInitialData is None:
            imageInitialData = self._defaultImageInitialData

        #Capture image initial and nodata to compare against
        self._imageInitialData = imageInitialData
        self._imageNoData = imageNoData
        mapop = self._rasterMapOpTestSupport.createRasterMapOp(tileIds, zoomLevel, tileSize, name, imageNoData, imageInitialData)
        return RasterMapOp(mapop=mapop, gateway=self._mrgeo.gateway, context=self._sparkContext)

    def createRasterMapOpWithBounds(self, tileIds, zoomLevel, tileSize, w, s, e, n, name='', imageNoData=None,
                                    imageInitialData=None):
        if imageNoData is None:
            imageNoData = self._defaultImageNoData
        if imageInitialData is None:
            imageInitialData = self._defaultImageInitialData

        bounds = self._jvm.Bounds(w, s, e, n)
        mapop = self._rasterMapOpTestSupport.createRasterMapOpWithBounds(tileIds, zoomLevel, tileSize, name,
                                                                         bounds, imageNoData, imageInitialData)
        return RasterMapOp(mapop=mapop, gateway=self._mrgeo.gateway, context=self._sparkContext)

    def getRDD(self, mapop):
        return self._rasterMapOpTestSupport.getRDD(mapop.mapop)

    # Verify the rasters in the RDD using the verifiers, which should be a dict mapping a tileId to a verifier for that
    # tiles Raster.  A verifier should be a function that takes a long and a Raster and returns nothing.  If a verifier
    # is associated with a key not found in the RDD, then the method should fail the test
    # if failOnMissingKey is true
    def verifyRasters(self, rdd, verifiers, failOnMissingKey = True):
        rasters = rdd.collect()
        # Rasters should be an array of tuples of TileIdWritable, and RasterWritable
        for raster in rasters:
            # Tile ID is the first element in the tuple
            tileId = raster._1().get()
            if tileId in verifiers:
                verifier = verifiers.pop(tileId, None)
                verifier(tileId, self._jvm.RasterWritable.toRaster(raster._2()))
        if (failOnMissingKey and len(verifiers) != 0):
            self.fail("Keys missing from RDD: {0}".format(verifiers.keys()))


    def verifyRastersAreUnchanged(self, rdd, tileIds, imageInitialData = None):
        verifier = partial(self.verifyRasterhasImageInitialData,
                           imageInitialData = self._imageInitialData if imageInitialData is None else imageInitialData)
        verifiers = dict()
        for tileId in tileIds:
            verifiers[tileId] = verifier
        self.verifyRasters(rdd, verifiers)

    def verifyRasterhasImageInitialData(self, tileId, raster, imageInitialData):
        width = raster.getWidth();
        height = raster.getHeight();
        bands = raster.getNumBands();

        # Loop over every band.
        for b in range(bands):
            self.assertTrue(self._rasterMapOpTestSupport.verifySamples(raster, 0, 0, width, height, b, imageInitialData[b]),
                            "Samples at band {0} do not match expected value of {1}".format(b, imageInitialData[b]))

        # Iterating over a large number of samples seems to cause the python gateway to hang as of 0.10.3
        # self.forEachSampleInRaster(raster, lambda b, x, y, s: (self.assertEquals(s, imageInitialData[b])))


    # Verifies the rasters in the RDD do not have data outside of the specified bounds, optionally verifying the
    # expectedData inside the bounds
    def verifyRastersNoData(self, rdd, tileIds, tileSize, zoomLevel, left, bottom, right, top,
                            nodatas = None, expectedData = None):
        if nodatas is None:
            nodatas = self._imageNoData
        verifier = partial(self.verifyRasterNoData, zoomLevel = zoomLevel, tileSize = tileSize,
                           left = left, right = right, top = top, bottom = bottom,
                           nodatas = nodatas, expectedData=expectedData)
        verifiers = dict()
        for tileId in tileIds:
            verifiers[tileId] = verifier
        self.verifyRasters(rdd, verifiers)


    # Verifies that the data outside the the specified bounds is nodata, optionally verifying that all other samples
    # are in accordance with expectedData
    def verifyRasterNoData(self, tileId, raster, zoomLevel, tileSize, left, right, top, bottom,
                           nodatas, expectedData = None):
        tile = self._jvm.TMSUtils.tileid(tileId, zoomLevel)
        tileX = tile.getTx()
        tileY = tile.getTy()
        # Convert lat lon bounds to tile pixel bounds
        pixelLeftBottom = self._jvm.TMSUtils.latLonToTilePixelUL(bottom, left, tileX, tileY, zoomLevel, tileSize)
        pixelRightTop = self._jvm.TMSUtils.latLonToTilePixelUL(top, right, tileX, tileY, zoomLevel, tileSize)
        lbPx = pixelLeftBottom.getPx()
        lbPy = pixelLeftBottom.getPy()
        rtPx = pixelRightTop.getPx()
        rtPy = pixelRightTop.getPy()

        bands = raster.getNumBands();

        #capture max and min for expected data check
        minX = 0
        minY = 0
        maxX = tileSize
        maxY = tileSize
        # Loop over every band.
        for b in range(bands):


            # Verify the top nodata rect in raster coords (0,0 at top left, + x to the right, +y down)
            if 0 < rtPy < tileSize:
                rtPy = int(rtPy)
                minY = rtPy
                self.assertTrue(self._rasterMapOpTestSupport.verifySamples(raster, 0, 0, tileSize, rtPy - 1, b,
                                                                           nodatas[b]),
                                "Raster did not have expected nodata above the top bounds")

            # Verify the bottom nodata rect in raster coords (0,0 at top left, + x to the right, +y down)
            if 0 < lbPy < tileSize:
                lbPy = int(lbPy)
                maxY = lbPy
                self.assertTrue(self._rasterMapOpTestSupport.verifySamples(raster, 0, lbPy + 1, tileSize,
                                                           tileSize-(lbPy + 1), b, nodatas[b]),
                                "Raster did not have expected nodata below the bottom bounds")

            # Verify the left nodata rect in raster coords (0,0 at top left, + x to the right, +y down)
            if 0 < lbPx < tileSize:
                lbPx = int(lbPx)
                minX = lbPx
                self.assertTrue(self._rasterMapOpTestSupport.verifySamples(raster, 0, minY, lbPx - 1,
                                                          maxY - minY, b, nodatas[b]),
                                "Raster did not have expected nodata left of the left bounds")

            # Verify the right nodata rect in raster coords (0,0 at top left, + x to the right, +y down)
            if 0 < rtPx < tileSize:
                rtPx = int(rtPx)
                maxX = rtPx
                self.assertTrue(self._rasterMapOpTestSupport.verifySamples(raster, rtPx + 1, minY,
                                                           tileSize - (rtPx + 1),
                                                           maxY - minY, b, nodatas[b]),
                                "Raster did not have expected nodata right the right bounds")
            # Verify expected data if specified
            if expectedData is not None:
                self.assertTrue(self._rasterMapOpTestSupport.verifySamples(raster, minX, minY, maxX - minX, maxY - minY,
                                                                           b, expectedData[b]),
                                "Raster did not have expected data within the bounds")


    def forEachSampleInRaster(self, raster, fun):
        width = raster.getWidth();
        height = raster.getHeight();
        bands = raster.getNumBands();

        # Loop over every sample.
        for b in range(bands):
            samples = self._getSamples(raster, width, height, b)
            offset = 0
            for y in range(width):
                for x in range(height):
                    fun(b, x, y, samples[offset])
                    offset += 1


    def _getSamples(self, raster, width, height, b):
        return raster.getSamples(0, 0, width, height, b, None)


    def _getArray(self, type, values = []):
        cnt = 0
        array = self._getEmptyArray(type, len(values))
        for value in values:
            array[cnt] = value
            cnt += 1
        return array

    def _getDoubleArray(self, values = []):
        return self._getArray(self._jvm.double, values)

    def _getEmptyArray(self, type, size):
        return self._mrgeo.gateway.new_array(type, size)

    def _getEmptyDoubleArray(self, size):
        return self._getEmptyArray(self._jvm.double, size)

    def _toJavaInt(self, longValue):
        return self._jvm.Long.intValue(longValue)
