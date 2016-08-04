import mrgeotest


class MrGeoIntegrationTests(mrgeotest.MrGeoTests):

    allones = None
    allhundreds = None
    smallelevation = None
    toblertiny = None

    @classmethod
    def setUpClass(cls):
        # cls.GENERATE_BASELINE_DATA = True

        super(MrGeoIntegrationTests, cls).setUpClass()

        # copy data to HDFS
        cls.copy("all-hundreds")
        cls.copy("all-ones")
        cls.copy("all-twos")
        cls.copy("small-elevation")
        cls.copy("tobler-raw-tiny")
        cls.copy("AmbulatoryPt.shp")
        cls.copy("AmbulatoryPt.prj")
        cls.copy("AmbulatoryPt.shx")
        cls.copy("AmbulatoryPt.dbf")

    def setUp(self):
        super(MrGeoIntegrationTests, self).setUp()

        self.allones = self.mrgeo.load_image("all-ones")
        self.alltwos = self.mrgeo.load_image("all-twos")
        self.allhundreds = self.mrgeo.load_image("all-hundreds")
        self.smallelevation = self.mrgeo.load_image("small-elevation")
        self.toblertiny = self.mrgeo.load_image("tobler-raw-tiny")

    def test_rasterize_vector(self):
        roads = self.mrgeo.load_vector("AmbulatoryPt.shp")
        result = roads.rasterizevector("MASK", "12z")
        self.compareraster(result, self.name)

    def test_add(self):
        add = self.allones + self.allhundreds
        self.compareraster(add, self.name)

    def test_add_procedural(self):
        add = self.mrgeo.add(self.allones, rasterB=self.allhundreds)
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

    def test_minus(self):
        sub = self.allones - self.allhundreds
        self.compareraster(sub, self.name)

    def test_minus_constA(self):
        sub = 1 - self.allhundreds
        self.compareraster(sub, self.name)

    def test_minus_constA_procedural(self):
        sub = self.mrgeo.rminus(const=1.0, raster=self.allhundreds)
        self.compareraster(sub, self.name)

    def test_minus_constB(self):
        sub = self.allhundreds - 1
        self.compareraster(sub, self.name)

    def test_minus_negconst(self):
        sub = self.allhundreds - -1
        self.compareraster(sub, self.name)

    def test_minusAlt(self):
        sub = self.allones.minus(self.allhundreds)
        self.compareraster(sub, self.name)

    def test_minusAlt_constA(self):
        sub = self.allhundreds.minus(const=1)
        self.compareraster(sub, self.name)

    def test_minusAlt_negconst(self):
        sub = self.allhundreds.minus(const=-1)
        self.compareraster(sub, self.name)

    def test_aspect(self):
        aspect = self.smallelevation.aspect()
        self.compareraster(aspect, self.name)

    def test_aspect_deg(self):
        aspect = self.smallelevation.aspect("deg")
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
    def test_bitwise_or(self):
        result = self.allhundreds.convert("byte", "truncate") | 6
        self.compareraster(result, self.name, nodata=255)

    def test_bitwise_and(self):
        result = self.allhundreds.convert("byte", "truncate") & 6
        self.compareraster(result, self.name, nodata=255)

    def test_bitwise_xor(self):
        result = self.allhundreds.convert("byte", "truncate") ^ 6
        self.compareraster(result, self.name, nodata=255)

    def test_bitwise_complement(self):
        result = ~self.allhundreds.convert("byte", "truncate")
        self.compareraster(result, self.name, nodata=255)

    def test_cos(self):
        cos = self.allones.cos()
        self.compareraster(cos, self.name)

    def test_crop(self):
        crop = self.smallelevation.crop(w=142.05, s=-17.75, e=142.2, n=-17.65)
        self.compareraster(crop, self.name)

    def test_crop_exact(self):
        crop = self.smallelevation.cropexact(w=142.05, s=-17.75, e=142.2, n=-17.65)
        self.compareraster(crop, self.name)

    def test_divide(self):
        div = self.allhundreds / self.allones
        self.compareraster(div, self.name)

    def test_divide_constA(self):
        div = self.allhundreds / 2.5
        self.compareraster(div, self.name)

    def test_divide_constB(self):
        div = 2.5 / self.allhundreds
        self.compareraster(div, self.name)

    def test_divideAlt(self):
        div = self.allhundreds.div(self.allones)
        self.compareraster(div, self.name)

    def test_divideAlt_constA(self):
        div = self.allhundreds.div(const=2.5)
        self.compareraster(div, self.name)

    def test_pow(self):
        p = self.allhundreds ** self.alltwos
        self.compareraster(p, self.name)

    def test_pow_constA(self):
        p = self.allhundreds ** 2
        self.compareraster(p, self.name)

    def test_pow_constB(self):
        p = 2 ** self.alltwos
        self.compareraster(p, self.name)

    def test_powAlt(self):
        p = self.allhundreds.pow(self.alltwos)
        self.compareraster(p, self.name)

    def test_powAlt_constA(self):
        p = self.allhundreds.pow(const=2)
        self.compareraster(p, self.name)

    def test_export(self):
        exp = self.smallelevation.export(self.outputdir + self.name, singleFile=True, format="tiff", overridenodata=-9999)

        self.comparelocalraster(self.name)
        self.compareraster(exp, self.name)

    def test_costdistance_two_points(self):
        cd = self.toblertiny.costdistance(-1.0, -1, 64.75, 30.158, 65.268, 29.983)
        self.compareraster(cd, self.name)

    def test_costdistance_with_point_list(self):
        points = [64.75, 30.158]
        cd = self.toblertiny.costdistance(-1.0, -1, points)
        self.compareraster(cd, self.name)

    def test_leastcostpath(self):
        points = [64.75, 30.158, 65.268, 29.983]
        cd = self.toblertiny.costdistance(-1.0, -1, 64.75, 30.158)
        destPoints = self.mrgeo.create_points([65.087, 30.194, 65.283, 29.939])
        lcp = destPoints.leastcostpath(cd)
        self.comparevector(lcp, self.name)
