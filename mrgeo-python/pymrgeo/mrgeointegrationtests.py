import mrgeotest


class MrGeoIntegrationTests(mrgeotest.MrGeoTests):

    allones = None
    allhundreds = None
    smallelevation = None

    @classmethod
    def setUpClass(cls):
        super(MrGeoIntegrationTests, cls).setUpClass()

        # copy data to HDFS
        cls.copy("all-hundreds")
        cls.copy("all-ones")

    def setUp(self):
        super(MrGeoIntegrationTests, self).setUp()

        self.allones = self.mrgeo.load_resource("all-ones")
        self.allhundreds = self.mrgeo.load_resource("all-hundreds")

    def test_add(self):
            print(self.name)
            add = self.allones + self.allhundreds

            self.compareraster(add, self.name)
