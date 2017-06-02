import sys

sys.path.append("..")  # need to add the parent test path...  yuck!
import mrgeotest

class MrGeoMetadataTests(mrgeotest.MrGeoTests):
    toblertiny = None
    roads = None

    @classmethod
    def setUpClass(cls):
        # cls.GENERATE_BASELINE_DATA = True

        super(MrGeoMetadataTests, cls).setUpClass()

        # copy data to HDFS
        cls.copy("tobler-raw-tiny")

        cls.copy("AmbulatoryPt.shp")
        cls.copy("AmbulatoryPt.prj")
        cls.copy("AmbulatoryPt.shx")
        cls.copy("AmbulatoryPt.dbf")

    def setUp(self):
        super(MrGeoMetadataTests, self).setUp()

        if self.toblertiny is None:
            self.toblertiny = self.mrgeo.load_image("tobler-raw-tiny")

        if self.roads is None:
            self.roads = self.mrgeo.load_vector("AmbulatoryPt.shp")

    def test_load_raster_metadata(self):
        meta = self.toblertiny.metadata()

        # print(meta)

        self.assertIsNotNone(meta, "Missing metadata")

        self.assertEqual(len(meta), 14, "number of metadata elements is different")

        self.assertEqual(meta['tilesize'], 512, "Tilesize is different")
        self.assertEqual(meta['maxZoomLevel'], 10, "maxZoomLevel is different")
        self.assertEqual(meta['bands'], 1, "bands is different")
        self.assertEqual(meta['protectionLevel'], "", "protectionLevel is different")
        self.assertIsNone(meta['quantiles'], "quantiles is not None")
        self.assertEqual(meta['tileType'], 4, "Tilesize is different")
        self.assertIsNone(meta['categories'], "categories is not None")
        self.assertEqual(meta['classification'], 'Continuous', "classification is different")
        self.assertIsNone(meta['resamplingMethod'],  "resamplingMethod is no None")

        bounds = meta['bounds']
        self.assertIsNotNone(bounds, "Missing bounds")

        self.assertEqual(len(bounds), 4, "number of bounds elements is different")
        self.assertAlmostEqual(bounds['w'], 64.93599700927734, 5, "bounds w is different")
        self.assertAlmostEqual(bounds['s'], 29.98699951171875, 5, "bounds s is different")
        self.assertAlmostEqual(bounds['e'], 65.16200256347656, 5, "bounds e is different")
        self.assertAlmostEqual(bounds['n'], 30.117000579833984, 5, "bounds n is different")

        stats = meta['stats']
        self.assertIsNotNone(stats, "Missing stats")
        self.assertEqual(len(stats), 1, "number of stats (array) elements is different")

        stats = stats[0] # get the single element
        self.assertIsNotNone(stats, "Missing stats")
        self.assertEqual(len(stats), 5, "number of stats elements is different")

        self.assertAlmostEqual(stats['min'], 0.7147477269172668, 5, "stats min is different")
        self.assertAlmostEqual(stats['max'], 4.190144062042236, 5, "stats max is different")
        self.assertAlmostEqual(stats['mean'], 0.8273760278752889, 5, "stats mean is different")
        self.assertAlmostEqual(stats['sum'], 433783.32290267944, 5, "stats sum is different")
        self.assertEqual(stats['count'], 524288, "stats count is different")

        imeta = meta['imageMetadata']
        self.assertIsNotNone(imeta, "Missing imageMetadata")
        self.assertEqual(len(imeta), 11, "number of imageMetadata elements is different")

        for i in range(0, 10):
            m = imeta[i]

            self.assertIsNotNone(m, "Missing imageMetadata entry")
            self.assertEqual(len(m), 4, "number of imageMetadata entry elements is different")

            self.assertIsNone(m['tileBounds'], "tileBounds is not None")
            self.assertIsNone(m['name'], " name is not None")
            self.assertIsNone(m['pixelBounds'], " pixelBounds is not None")
            self.assertIsNone(m['stats'], " stats is not None")

        m = imeta[10]

        self.assertIsNotNone(m, "Missing imageMetadata entry")
        self.assertEqual(len(m), 4, "number of imageMetadata entry elements is different")

        self.assertIsNotNone(m['name'], " name is None")
        self.assertEqual(m['name'], '10', "name is different")

        self.assertIsNotNone(m['tileBounds'], "tileBounds is None")
        tb = m['tileBounds']

        self.assertEqual(len(tb), 4, "number of tileBounds elements is different")
        self.assertEqual(tb['minX'], 696, "minX is different")
        self.assertEqual(tb['minY'], 341, "minY is different")
        self.assertEqual(tb['maxX'], 697, "maxX is different")
        self.assertEqual(tb['maxY'], 341, "maxY is different")

        self.assertIsNotNone(m['pixelBounds'], "pixelBounds is None")
        pb = m['pixelBounds']

        self.assertEqual(len(pb), 4, "number of pixelBounds elements is different")
        self.assertEqual(pb['minX'], 0, "minX is different")
        self.assertEqual(pb['minY'], 0, "minY is different")
        self.assertEqual(pb['maxX'], 330, "maxX is different")
        self.assertEqual(pb['maxY'], 190, "maxY is different")

        self.assertIsNotNone(m['stats'], " stats is None")
        stats = m['stats']

        self.assertEqual(len(stats), 1, "number of stats (array) elements is different")

        stats = stats[0] # get the single element
        self.assertIsNotNone(stats, "Missing stats")

        self.assertEqual(len(stats), 5, "number of stats elements is different")

        self.assertAlmostEqual(stats['min'], 0.7147477269172668, 5, "stats min is different")
        self.assertAlmostEqual(stats['max'], 4.190144062042236, 5, "stats max is different")
        self.assertAlmostEqual(stats['mean'], 0.8273760278752889, 5, "stats mean is different")
        self.assertAlmostEqual(stats['sum'], 433783.32290267944, 5, "stats sum is different")
        self.assertEqual(stats['count'], 524288, "stats count is different")

    def test_load_vector_metadata(self):
        meta = self.roads.metadata()

        self.assertIsNone(meta, "Vector metadata should be None until it is completely implemented")



