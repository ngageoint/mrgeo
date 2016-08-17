import os
import shutil
from osgeo import gdal
from unittest import TestSuite, TestCase, defaultTestLoader, main

import sys
from py4j.java_gateway import java_import

import gdaltest
from pymrgeo.instance import is_instance_of
from pymrgeo.mrgeo import MrGeo


class MrGeoTests(TestCase):

    GENERATE_BASELINE_DATA = False

    classname = None
    mrgeo = None
    gateway = None

    _CWD = os.getcwd()
    _OUTPUT = "output"
    _OUTPUT_HDFS = None
    _OUTPUT_BASE = "/mrgeo/test-files/output/"
    _INPUT = "testFiles"
    _INPUT_HDFS = None
    _INPUT_BASE = "/mrgeo/test-files/"

    inputdir = None
    inputhdfs = None
    outputdir = None
    outputhdfs = None

    def compareraster(self, raster, testname, nodata=-9999):
        if self.GENERATE_BASELINE_DATA:
            self.saveraster(raster, testname, nodata)
        else:
            # jvm = self.gateway.jvm
            # test = raster.mapop.toDataset(False)

            testimage = self.outputdir + testname
            raster.export(testimage, singleFile=True, format="tiff", overridenodata=nodata)
            testimage += ".tif"
            test = gdal.Open(testimage)

            golden = gdal.Open(self.inputdir + testname + ".tif")

            # compare as GDAL Datasets.
            gdaltest.compare_db(self, golden, test)

            os.remove(testimage)

    def comparelocalraster(self, testname):
        if not self.GENERATE_BASELINE_DATA:
            golden = gdal.Open(self.inputdir + testname + ".tif")
            test = gdal.Open(self.outputdir + testname + ".tif")

            # compare as GDAL Datasets.
            gdaltest.compare_db(self, golden, test)

    def saveraster(self, raster, testname, nodata=-9999):
        name = self.inputdir + testname
        raster.export(name, singleFile=True, format="tiff", overridenodata=nodata)

    def savevector(self, vector, testname):
        name = self.inputdir + testname + ".tsv"
        vector.save(name)

    def comparevector(self, vector, testname):
        if self.GENERATE_BASELINE_DATA:
            self.savevector(vector, str(testname))
        else:
            jvm = self.mrgeo._get_jvm()
            # test = raster.mapop.toDataset(False)
            java_import(jvm, "org.mrgeo.hdfs.vector.DelimitedVectorReader")

            testvector = str(self.outputhdfs + testname + ".tsv")
            vector.ssave(testvector)
            expectedvector = str(self.inputdir + testname + ".tsv")
            vdp_expected = jvm.DataProviderFactory.getVectorDataProvider(
                expectedvector,
                jvm.DataProviderFactory.AccessMode.READ,
                jvm.HadoopUtils.createConfiguration())
            expected_geom_reader = vdp_expected.getVectorReader().get()

            vdp = jvm.DataProviderFactory.getVectorDataProvider(
                testvector,
                jvm.DataProviderFactory.AccessMode.READ,
                jvm.HadoopUtils.createConfiguration())
            self.assertTrue(vdp is not None)
            vector_reader = vdp.getVectorReader()
            self.assertTrue(vector_reader is not None)
            self.assertTrue(is_instance_of(self.mrgeo.gateway, vector_reader, jvm.DelimitedVectorReader))
            self.assertEquals(vdp_expected.getVectorReader().count(), vector_reader.count())
            geom_reader = vector_reader.get()
            self.assertTrue(geom_reader is not None)

            while expected_geom_reader.hasNext():
                expected_geom = expected_geom_reader.next()
                geom = geom_reader.next()
                self.assertTrue(geom is not None)
                self.assertEquals(expected_geom.type(), geom.type())
                self.assertAlmostEquals(float(expected_geom.getAttribute("COST_S")),
                                        float(geom.getAttribute("COST_S")), delta=0.001)
                self.assertAlmostEquals(float(expected_geom.getAttribute("DISTANCE_M")),
                                        float(geom.getAttribute("DISTANCE_M")), delta=0.001)
                self.assertAlmostEquals(float(expected_geom.getAttribute("MINSPEED_MPS")),
                                        float(geom.getAttribute("MINSPEED_MPS")), delta=0.001)
                self.assertAlmostEquals(float(expected_geom.getAttribute("MAXSPEED_MPS")),
                                        float(geom.getAttribute("MAXSPEED_MPS")), delta=0.001)
                self.assertAlmostEquals(float(expected_geom.getAttribute("AVGSPEED_MPS")),
                                        float(geom.getAttribute("AVGSPEED_MPS")), delta=0.001)

            # Should not be any more geometries in the actual output
            self.assertFalse(geom_reader.hasNext())
            jvm.HadoopFileUtils.delete(testvector)

    @classmethod
    def copy(cls, srcfile, srcpath=None, dstpath=None, dstfile=None):
        jvm = cls.mrgeo._get_jvm()
        java_import(jvm, "org.mrgeo.hdfs.utils.HadoopFileUtils")
        java_import(jvm, "org.apache.hadoop.fs.Path")

        if srcpath is not None:
            src = srcpath
            if not src.endswith('/'):
                src += '/'
            src += srcfile
        else:
            src = srcfile

        if not os.path.exists(src):
            if os.path.exists(cls.inputdir + src):
                src = cls.inputdir + src

        if not os.path.exists(src):
            raise Exception("Source (" + src + ") is not a file or directory")

        if dstfile is not None:
            dst = dstfile
            if not dst.endswith('/'):
                dst += '/'
            dst += dstfile

            if not os.path.isfile(src):
                raise Exception("Source (" + src + ") is must be a file")

            if jvm.HadoopFileUtils.exists(dst):
                jvm.HadoopFileUtils.delete(dst)

            jvm.HadoopFileUtils.copyFileToHdfs(src, dst)

            return dst
        elif dstpath is not None:
            dst = dstpath
        else:
            dst = cls.inputhdfs

        basefile = os.path.basename(src)
        dstfile = dst + basefile

        if jvm.HadoopFileUtils.exists(dstfile):
            jvm.HadoopFileUtils.delete(dstfile)

        jvm.HadoopFileUtils.copyToHdfs(src, dst)

        return dstfile

    @classmethod
    def setUpClass(cls):
        cls.classname = cls.__name__

        # print(cls.classname + " setup")

        cls.mrgeo = MrGeo()

        jvm = cls.mrgeo._get_jvm()
        java_import(jvm, "org.apache.hadoop.conf.Configuration")
        java_import(jvm, "org.apache.hadoop.fs.Path")
        java_import(jvm, "org.mrgeo.data.DataProviderFactory")
        java_import(jvm, "org.mrgeo.data.vector.VectorDataProvider")
        java_import(jvm, "org.mrgeo.data.vector.VectorReader")
        java_import(jvm, "org.mrgeo.hdfs.vector.DelimitedVectorReader")

        fs = jvm.HadoopFileUtils.getFileSystem()
        p = jvm.Path(cls._INPUT_BASE).makeQualified(fs)
        cls._INPUT_HDFS = p

        p = jvm.Path(cls._OUTPUT_BASE).makeQualified(fs)
        cls._OUTPUT_HDFS = p

        basedir = os.getenv('BASEDIR', '.')
        dirname = os.path.abspath(basedir)
        try:
            while True:
                names = os.listdir(dirname)
                if cls._INPUT in names:
                    break
                dirname = os.path.abspath(os.path.join(dirname, os.pardir))
        except OSError:
            pass

        basedir = os.path.abspath(dirname)

        cls.inputdir = os.path.abspath(basedir + '/' + cls._INPUT + "/" + cls.classname) + '/'
        cls.outputdir = os.path.abspath(basedir + '/' + cls._INPUT + '/' + cls._OUTPUT + "/" + cls.classname) + '/'

        cls.inputhdfs = jvm.Path(cls._INPUT_HDFS, "python/" + cls.classname).makeQualified(fs).toString() + '/'
        cls.outputhdfs = jvm.Path(cls._OUTPUT_HDFS, "python/" + cls.classname).makeQualified(fs).toString() + '/'

        if not os.path.exists(cls.inputdir):
            os.makedirs(cls.inputdir)

        if os.path.exists(cls.outputdir):
            shutil.rmtree(cls.outputdir, ignore_errors=True)

        if not os.path.exists(cls.outputdir):
            os.makedirs(cls.outputdir)

        jvm.HadoopFileUtils.create(cls.inputhdfs)

        if jvm.HadoopFileUtils.exists(cls.outputhdfs):
            jvm.HadoopFileUtils.cleanDirectory(cls.outputhdfs)

        jvm.HadoopFileUtils.create(cls.outputhdfs)

        jvm.MrGeoProperties.getInstance().setProperty(jvm.MrGeoConstants.MRGEO_HDFS_IMAGE, cls.inputhdfs)
        jvm.MrGeoProperties.getInstance().setProperty(jvm.MrGeoConstants.MRGEO_HDFS_VECTOR, cls.inputhdfs)

        jvm.LoggingUtils.setDefaultLogLevel(jvm.LoggingUtils.ERROR)

    @classmethod
    def tearDownClass(cls):
        cls.mrgeo.disconnect()

    def setUp(self):
        self.name = self._testMethodName

        self._doublebox("Starting", self.classname + ":" + self.name)
        self.mrgeo.usedebug()
        self.mrgeo.start()

        jvm = self.mrgeo._get_jvm()

        jvm.MrGeoProperties.getInstance().setProperty(jvm.MrGeoConstants.MRGEO_HDFS_IMAGE, self.inputhdfs)
        jvm.MrGeoProperties.getInstance().setProperty(jvm.MrGeoConstants.MRGEO_HDFS_VECTOR, self.inputhdfs)

    def tearDown(self):
        self.mrgeo.stop()
        self._doublebox("Test Finished", self.classname + ":" + self.name)

    def debug_logging(self):
        jvm = self.mrgeo._get_jvm()
        jvm.LoggingUtils.setDefaultLogLevel(jvm.LoggingUtils.DEBUG)

    def info_logging(self):
        jvm = self.mrgeo._get_jvm()
        jvm.LoggingUtils.setDefaultLogLevel(jvm.LoggingUtils.INFO)

    def warn_logging(self):
        jvm = self.mrgeo._get_jvm()
        jvm.LoggingUtils.setDefaultLogLevel(jvm.LoggingUtils.WARN)

    def error_logging(self):
        jvm = self.mrgeo._get_jvm()
        jvm.LoggingUtils.setDefaultLogLevel(jvm.LoggingUtils.ERROR)

    @staticmethod
    def _doublebox(text, name):

        sys.stdout.flush()

        width = len(name)
        if width < len(text):
            width = len(text)

        fmt = "{:*<" + str(width + 4) + "}"
        print(fmt.format(""))
        fmt = "{:<" + str(width + 2) + "}"
        print(fmt.format("*") + " *")
        fmt = "{:<" + str(width) + "}"
        print("* " + fmt.format(text) + " *")
        fmt = "{:<" + str(width + 2) + "}"
        print(fmt.format("*") + " *")
        fmt = "{:*<" + str(width + 4) + "}"
        print(fmt.format(""))
        fmt = "{:<" + str(width) + "}"
        print("* " + fmt.format(name) + " *")
        fmt = "{:*<" + str(width + 4) + "}"
        print(fmt.format(""))
        print("")

        sys.stdout.flush()


class VectorTestExpectation:
    def __init__(self, cost, distance, minSpeed, maxSpeed, avgSpeed):
        self.cost = cost
        self.distance = distance
        self.minSpeed = minSpeed
        self.maxSpeed = maxSpeed
        self.avgSpeed = avgSpeed


