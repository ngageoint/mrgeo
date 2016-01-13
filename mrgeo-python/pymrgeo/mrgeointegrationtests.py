import mrgeotest


class MrGeoIntegrationTests(mrgeotest.MrGeoTests):

    allones = None
    allhundreds = None
    smallelevation = None

    @classmethod
    def setUpClass(cls):
        # cls.GENERATE_BASELINE_DATA = True

        super(MrGeoIntegrationTests, cls).setUpClass()

        # copy data to HDFS
        cls.copy("all-hundreds")
        cls.copy("all-ones")
        cls.copy("small-elevation")

    def setUp(self):
        super(MrGeoIntegrationTests, self).setUp()

        self.allones = self.mrgeo.load_resource("all-ones")
        self.allhundreds = self.mrgeo.load_resource("all-hundreds")
        self.smallelevation = self.mrgeo.load_resource("small-elevation")

    def test_add(self):
        add = self.allones + self.allhundreds
        self.compareraster(add, self.name)

    def test_add_constA(self):
        add = 1 + self.allhundreds
        self.compareraster(add, self.name)

    def test_add_constB(self):
        add = self.allhundreds + 1
        self.compareraster(add, self.name)

    def test_add_negconst(self):
        add = self.allhundreds + -1
        self.compareraster(add, self.name)

    def test_addAlt(self):
        add = self.allones.add(self.allhundreds)
        self.compareraster(add, self.name)

    def test_addAlt_constA(self):
        add = self.allhundreds.add(const=1)
        self.compareraster(add, self.name)

    def test_addAlt_negconst(self):
        add = self.allhundreds.add(const=-1)
        self.compareraster(add, self.name)

    def test_aspect(self):
        aspect = self.smallelevation.aspect()
        self.compareraster(aspect, self.name)

    def test_aspect_deg(self):
        aspect = self.smallelevation.aspect("deg")
        self.compareraster(aspect, self.name)

    def test_aspect_gradient(self):
        aspect = self.smallelevation.aspect("gradient")
        self.compareraster(aspect, self.name)

    def test_aspect_percent(self):
        aspect = self.smallelevation.aspect("percent")
        self.compareraster(aspect, self.name)

    def test_aspect_rad(self):
        aspect = self.smallelevation.aspect("rad")
        self.compareraster(aspect, self.name)

    def test_bandcombine(self):
        bands = self.allhundreds.bandcombine(self.allones)
        self.compareraster(bands, self.name)

    def test_bandcombine3(self):
        bands = self.allhundreds.bandcombine(self.allones, self.smallelevation)
        self.compareraster(bands, self.name)

    def test_bandcombineAlt(self):
        bands = self.allhundreds.bc(self.allones)
        self.compareraster(bands, self.name)

    def test_cos(self):
        cos = self.allones.cos()
        self.compareraster(cos, self.name)

    def test_crop(self):
        crop = self.smallelevation.crop(142.05, -17.75, 142.2, -17.65)
        self.compareraster(crop, self.name)

    def test_crop_exact(self):
        crop = self.smallelevation.cropexact(142.05, -17.75, 142.2, -17.65)
        self.compareraster(crop, self.name)




