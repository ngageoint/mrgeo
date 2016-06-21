from __future__ import print_function

import sys
from threading import Lock

from py4j.java_gateway import java_import

import constants
import mapopgenerator
import java_gateway
from rastermapop import RasterMapOp
from vectormapop import VectorMapOp

# python3 doesn't have a long type, this masks long to int
if sys.version_info > (3,):
    long = int


class MrGeo(object):
    gateway = None
    gateway_client = None
    lock = Lock()

    sparkContext = None
    job = None

    _host = None
    _port = None

    def __init__(self, host=None, port=None):
        self._host = host
        self._port = port
        self._ensure_gateway_initialized(self._host, self._port)
        try:
            self.initialize()
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    def _ensure_gateway_initialized(self, host=None, port=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with self.lock:
            if not self.gateway:
                self.gateway, self.gateway_client = java_gateway.launch_gateway(host, port)
                self.jvm = self.gateway.jvm

    def _create_job(self):
        jvm = self.gateway.jvm
        java_import(jvm, "org.mrgeo.data.DataProviderFactory")
        java_import(jvm, "org.mrgeo.job.*")
        java_import(jvm, "org.mrgeo.utils.DependencyLoader")
        java_import(jvm, "org.mrgeo.utils.StringUtils")

        appname = "PyMrGeo"

        self.job = jvm.JobArguments()
        java_gateway.set_field(self.job, "name", appname)

        # Yarn in the default
        self.useyarn()

    def initialize(self):
        self._create_job()
        mapopgenerator.generate(self.gateway, self.gateway_client)

    def usedebug(self):
        self.job.useDebug()

    def useyarn(self):
        self.job.useYarn()

    def start(self):
        self._ensure_gateway_initialized(self._host, self._port)

        jvm = self.gateway.jvm

        job = self.job

        job.addMrGeoProperties()
        dpf_properties = jvm.DataProviderFactory.getConfigurationFromProviders()

        for prop in dpf_properties:
            job.setSetting(prop, dpf_properties[prop])

        if job.isYarn():
            job.loadYarnSettings()

        java_gateway.set_field(job, "jars",
                  jvm.StringUtils.concatUnique(
                      jvm.DependencyLoader.getAndCopyDependencies("org.mrgeo.mapalgebra.MapAlgebra", None),
                      jvm.DependencyLoader.getAndCopyDependencies(jvm.MapOpFactory.getMapOpClassNames(), None)))

        conf = jvm.MrGeoDriver.prepareJob(job)

        if job.isYarn():
            # need to override the yarn mode to "yarn-client" for python
            conf.set("spark.master", "yarn-client")

            if not conf.getBoolean("spark.dynamicAllocation.enabled", False):
                conf.set("spark.executor.instances", str(job.executors()))

            conf.set("spark.executor.cores", str(job.cores()))

            mem = job.memoryKb()
            overhead = mem * 0.1
            if overhead < 384:
                overhead = 384

            mem -= (overhead * 2)  # overhead is 1x for driver and 1x for application master (am)
            conf.set("spark.executor.memory", jvm.SparkUtils.kbtohuman(long(mem), "m"))

        jsc = jvm.JavaSparkContext(conf)
        jsc.setCheckpointDir(jvm.HadoopFileUtils.createJobTmp(jsc.hadoopConfiguration()).toString())
        self.sparkContext = jsc.sc()

        # print("started")

    def stop(self):
        if self.sparkContext:
            self.sparkContext.stop()
            self.sparkContext = None
        if self.gateway:
            self.gateway.shutdown()
            self.gateway = None
            self.gateway_client = None
        java_gateway.terminate()

    def list_images(self):
        jvm = self.gateway.jvm

        pstr = self.job.getSetting(constants.provider_properties, "")
        pp = jvm.ProviderProperties.fromDelimitedString(pstr)

        rawimages = jvm.DataProviderFactory.listImages(pp)

        images = []
        for image in rawimages:
            images.append(str(image))

        return images

    def load_image(self, name):
        jvm = self.gateway.jvm

        pstr = self.job.getSetting(constants.provider_properties, "")
        pp = jvm.ProviderProperties.fromDelimitedString(pstr)

        dp = jvm.DataProviderFactory.getMrsImageDataProvider(name, jvm.DataProviderFactory.AccessMode.READ, pp)

        mapop = jvm.MrsPyramidMapOp.apply(dp)
        mapop.context(self.sparkContext)

        # print("loaded " + name)

        return RasterMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self.job)

    def ingest_image(self, name, zoom=None, categorical=None):

        jvm = self.gateway.jvm

        if zoom is None and categorical is None:
            mapop = jvm.IngestImageMapOp.create(name)
        elif zoom is None and categorical is not None:
            mapop = jvm.IngestImageMapOp.create(name, categorical)
        elif zoom is not None and categorical is None:
            mapop = jvm.IngestImageMapOp.create(name, zoom)
        else:
            mapop = jvm.IngestImageMapOp.create(name, zoom, categorical)

        if (mapop.setup(self.job, self.sparkContext.getConf()) and
                mapop.execute(self.sparkContext) and
                mapop.teardown(self.job, self.sparkContext.getConf())):
            return RasterMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self.job)
        return None

    def create_points(self, coords):
        jvm = self.gateway.jvm

        elements = []
        for coord in coords:
            if isinstance(coord, list):
                for c in coord:
                    elements.append(c)
            else:
                elements.append(coord)

        # Convert from a python list to a Java array
        cnt = 0
        array = self.gateway.new_array(self.gateway.jvm.double, len(elements))
        for element in elements:
            array[cnt] = element
            cnt += 1

        mapop = jvm.PointsMapOp.apply(array)
        mapop.context(self.sparkContext)

        return VectorMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self.job)
