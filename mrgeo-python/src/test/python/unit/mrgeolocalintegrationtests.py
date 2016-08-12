from py4j.java_gateway import java_import
from unittest import TestLoader, TestCase, TextTestRunner
from pymrgeo.mrgeo import MrGeo
from rastermapoptestsupport import RasterMapOpTestSupport


class MrGeoLocalIntegrationTests(TestCase):

    @classmethod
    def setUpClass(cls):
        cls._mrgeo = MrGeo()

    def setUp(self):
        mrgeo = self._mrgeo

        # Get the JVM.  This will create the gateway
        self._jvm = mrgeo._get_jvm()
        mrgeo.usedebug()
        mrgeo.start()

        self._sparkContext = mrgeo.sparkContext

        self._mrgeo = mrgeo
        self._rasterMapOpTestSupport = RasterMapOpTestSupport(self._mrgeo)
        self.createDefaultRaster()

    def createDefaultRaster(self):
        tileIds = self._getArray(self._jvm.long, 4)
        tileIds[0] = 11
        tileIds[1] = 12
        tileIds[2] = 19
        tileIds[3] = 20
        self._tileIds = tileIds
        imageInitialData = self._getArray(self._jvm.double, 1)
        imageInitialData[0] = 1.0
        self._imageInitialData = imageInitialData
        self._inputRaster = self._rasterMapOpTestSupport.createRasterMapOp(tileIds, 3, 512, imageInitialData = imageInitialData)

    def test_crop_with_explicit_bounds(self):
        cropresult = self._inputRaster.crop(w=-44.0, s=-44.0, e=44.0, n=44.0)
        croppedRDD = self._rasterMapOpTestSupport.getRDD(cropresult)
        count =  croppedRDD.count()
        self.assertEqual(4, count)
        self._rasterMapOpTestSupport.verifyRastersAreUnchanged(croppedRDD, [11, 12, 19, 20])

    def test_crop_with_raster_bounds(self):
        w, s, e, n = (-90.0, -90.0, 0.0, 90.0)
        boundsRaster = self._rasterMapOpTestSupport.createRasterMapOpWithBounds(self._tileIds, 3, 512, w, s, e, n,
                                                                                imageInitialData = self._imageInitialData)
        cropresult = self._inputRaster.crop(boundsRaster)
        croppedRDD = self._rasterMapOpTestSupport.getRDD(cropresult)
        count =  croppedRDD.count()
        self.assertEqual(2, count)
        self._rasterMapOpTestSupport.verifyRastersNoData(croppedRDD, [11, 19], 512, 3, left = w, bottom=s,
                                                         right=e, top=n, expectedData= self._imageInitialData)

    def test_cropexact_with_explicit_bounds(self):
        w, s, e, n = (-35.0, -35.0, 35.0, 35.0)
        cropresult = self._inputRaster.cropexact(w = w, s = s, e = e, n = n)
        croppedRDD = self._rasterMapOpTestSupport.getRDD(cropresult)
        count =  croppedRDD.count()
        self.assertEqual(4, count)
        self._rasterMapOpTestSupport.verifyRastersNoData(croppedRDD, [11, 12, 19, 20], 512, 3,
                                                         left=w, bottom=s, right=e, top=n,
                                                         expectedData = self._imageInitialData)

    def test_cropexact_with_raster_bounds(self):
        w, s, e, n = (-90.0, -90.0, 0.0, 90.0)
        boundsRaster = self._rasterMapOpTestSupport.createRasterMapOpWithBounds(self._tileIds, 3, 512, w, s, e, n,
                                                                                imageInitialData = self._imageInitialData)
        cropresult = self._inputRaster.cropexact(boundsRaster)
        croppedRDD = self._rasterMapOpTestSupport.getRDD(cropresult)
        count =  croppedRDD.count()
        self.assertEqual(2, count)
        self._rasterMapOpTestSupport.verifyRastersNoData(croppedRDD, [11, 19], 512, 3, left = w, bottom=s,
                                                         right=e, top=n, expectedData= self._imageInitialData)

    def tearDown(self):
        self._mrgeo.stop()

    def _getArray(self, type, size):
        return self._mrgeo.gateway.new_array(type, size)

    def _getSome(self, value):
        java_import(self.jvm, "scala.Some")
        return self.jvm.Some(value)


suite = TestLoader().loadTestsFromTestCase(MrGeoLocalIntegrationTests)
TextTestRunner(verbosity=2).run(suite)