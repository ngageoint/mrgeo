import unittest

from pymrgeo import MrGeo
import gdaltest

from os import getcwd, path, makedirs, remove
from osgeo import gdal
from py4j.java_gateway import java_import
import shutil


class MrGeoTests(unittest.TestCase):

    GENERATE_BASELINE_DATA = False

    classname = None
    mrgeo = None
    gateway = None

    _CWD = getcwd()
    _OUTPUT = "testFiles/output/"
    _OUTPUT_HDFS = None
    _OUTPUT_BASE = "/mrgeo/test-files/output/"
    _INPUT = "testFiles/"
    _INPUT_HDFS = None
    _INPUT_BASE = "/mrgeo/test-files/"

    inputdir = None
    inputhdfs = None
    outputdir = None
    outputhdfs = None

    def compareraster(self, raster, testname):
        if self.GENERATE_BASELINE_DATA:
            self.saveraster(raster, testname)
        else:
            # jvm = self.gateway.jvm
            # test = raster.mapop.toDataset(False)

            testimage = self.outputdir + "testimage"
            raster.export(testimage, singleFile=True, format="tiff", overridenodata=-9999)
            testimage += ".tif"
            test = gdal.Open(testimage)

            golden = gdal.Open(self.inputdir + testname + ".tif")

            # compare as GDAL Datasets.
            gdaltest.compare_db(self, golden, test)

            remove(testimage)

    def comparelocalraster(self, testname):
        if not self.GENERATE_BASELINE_DATA:
            golden = gdal.Open(self.inputdir + testname + ".tif")
            test = gdal.Open(self.outputdir + testname + ".tif")

            # compare as GDAL Datasets.
            gdaltest.compare_db(self, golden, test)


    def saveraster(self, raster, testname):
        name = self.inputdir + testname
        raster.export(name, singleFile=True, format="tiff", overridenodata=-9999)

    @classmethod
    def copy(cls, srcfile, srcpath=None, dstpath=None, dstfile=None):
        jvm = cls.gateway.jvm
        java_import(jvm, "org.mrgeo.hdfs.utils.HadoopFileUtils")
        java_import(jvm, "org.apache.hadoop.fs.Path")

        if srcpath is not None:
            src =  srcpath
            if not src.endswith('/'):
                src += '/'
            src += srcfile
        else:
            src = srcfile

        if not path.exists(src):
            if path.exists(cls.inputdir + src):
                src = cls.inputdir + src

        if not path.exists(src):
            raise Exception("Source (" + src + ") is not a file or directory")

        if dstfile is not None:
            dst =  dstfile
            if not dst.endswith('/'):
                dst += '/'
            dst += dstfile

            if not path.isfile(src):
                raise Exception("Source (" + src + ") is must be a file")

            if jvm.HadoopFileUtils.exists(dst):
                jvm.HadoopFileUtils.delete(dst)

            jvm.HadoopFileUtils.copyFileToHdfs(src, dst)

            return dst
        elif dstpath is not None:
            dst = dstpath
        else:
            dst = cls.inputhdfs

        basefile = path.basename(src)
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
        cls.gateway = cls.mrgeo.gateway

        jvm = cls.gateway.jvm
        java_import(jvm, "org.mrgeo.core.MrGeoConstants")
        java_import(jvm, "org.mrgeo.core.MrGeoProperties")
        java_import(jvm, "org.mrgeo.hdfs.utils.HadoopFileUtils")
        java_import(jvm, "org.apache.hadoop.fs.Path")
        java_import(jvm, "org.mrgeo.utils.LoggingUtils")

        fs = jvm.HadoopFileUtils.getFileSystem()
        p = jvm.Path(cls._INPUT_BASE).makeQualified(fs)
        cls._INPUT_HDFS = p

        p = jvm.Path(cls._OUTPUT_BASE).makeQualified(fs)
        cls._OUTPUT_HDFS = p

        cls.inputdir = path.abspath(cls._INPUT + "/" + cls.classname) + '/'
        cls.outputdir = path.abspath(cls._OUTPUT + "/" + cls.classname) + '/'

        cls.inputhdfs = jvm.Path(cls._INPUT_HDFS, "python/" + cls.classname).makeQualified(fs).toString() + '/'
        cls.outputhdfs = jvm.Path(cls._OUTPUT_HDFS, "python/" + cls.classname).makeQualified(fs).toString() + '/'

        if not path.exists(cls.inputdir):
            makedirs(cls.inputdir)

        if path.exists(cls.outputdir):
            shutil.rmtree(cls.outputdir, ignore_errors=True)

        if not path.exists(cls.outputdir):
            makedirs(cls.outputdir)

        jvm.HadoopFileUtils.create(cls.inputhdfs)

        if jvm.HadoopFileUtils.exists(cls.outputhdfs):
            jvm.HadoopFileUtils.cleanDirectory(cls.outputhdfs)

        jvm.HadoopFileUtils.create(cls.outputhdfs)

        jvm.MrGeoProperties.getInstance().setProperty(jvm.MrGeoConstants.MRGEO_HDFS_IMAGE, cls.inputhdfs)
        jvm.MrGeoProperties.getInstance().setProperty(jvm.MrGeoConstants.MRGEO_HDFS_VECTOR, cls.inputhdfs)

        jvm.LoggingUtils.setDefaultLogLevel(jvm.LoggingUtils.ERROR)

    def setUp(self):
        self.name = self._testMethodName

        self._doublebox("Starting", self.classname + ":" + self.name)
        self.mrgeo.usedebug()
        self.mrgeo.start()

    def tearDown(self):
        self.mrgeo.stop()
        self._doublebox("Test Finished", self.classname + ":" + self.name)


    def _doublebox(self, text, name):
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

if __name__ == '__main__':
    print('running tests')
    unittest.main()
