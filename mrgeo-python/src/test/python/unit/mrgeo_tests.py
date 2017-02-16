from unittest import TestCase

from pymrgeo.mrgeo import MrGeo


class MrGeoStartTests(TestCase):
    mrgeo = None

    @classmethod
    def setUpClass(cls):
        print("*** MrGeoStartTests.setUpClass()")
        cls.mrgeo = MrGeo()

    @classmethod
    def tearDownClass(cls):
        print("*** MrGeoStartTests.tearDownClass()")
        cls.mrgeo.disconnect()

    def setUp(self):
        mrgeo = self.mrgeo

        # Get the JVM.  This will create the gateway
        self._jvm = mrgeo._get_jvm()
        mrgeo.usedebug()

        # Don't want to automatically start, we're testing that functionality
        # mrgeo.start()
        # self._sparkContext = mrgeo.sparkContext

    def tearDown(self):
        # self.mrgeo.stop()
        pass

    def test_list_images_without_start(self):
        images = self.mrgeo.list_images()

        self.assertEqual(images, None, "Should be None")

    def test_load_image_without_start(self):
        images = self.mrgeo.load_image("foo")

        self.assertEqual(images, None, "Should be None")

    def test_ingest_image_without_start(self):
        images = self.mrgeo.ingest_image("foo")

        self.assertEqual(images, None, "Should be None")

    def test_load_vector_without_start(self):
        images = self.mrgeo.load_vector("Foo")

        self.assertEqual(images, None, "Should be None")

    def test_create_points_without_start(self):
        images = self.mrgeo.create_points([])

        self.assertEqual(images, None, "Should be None")
