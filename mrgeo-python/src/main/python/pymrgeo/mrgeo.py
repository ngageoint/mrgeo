from __future__ import print_function

import sys
from threading import Lock

from py4j.java_gateway import java_import

import constants
import java_gateway
import mapopgenerator
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

    _localGateway = False
    _job = None

    _host = None
    _port = None

    _started = False

    def __init__(self, host=None, port=None, pysparkContext=None):
        self._host = host
        self._port = port
        self._initialize(pysparkContext)

    def _create_gateway(self, host=None, port=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with self.lock:
            if not self.gateway:
                print("Initializing gateway")
                sys.stdout.flush()

                self.gateway, self.gateway_client = java_gateway.launch_gateway(host, port)

                jvm = self._get_jvm()

            else:
                print("Gateway already initialized")
                sys.stdout.flush()

    def _create_job(self):
        if not self._job:
            jvm = self._get_jvm()
            java_import(jvm, "org.mrgeo.job.*")

            appname = "PyMrGeo"

            self._job = jvm.JobArguments()
            java_gateway.set_field(self._job, "name", appname)

            # Yarn in the default
            self.useyarn()

    def _general_imports(self):
        jvm = self._get_jvm()

        java_import(jvm, "org.mrgeo.data.DataProviderFactory")
        java_import(jvm, "org.mrgeo.hdfs.utils.HadoopFileUtils")
        java_import(jvm, "org.mrgeo.core.*")
        java_import(jvm, "org.mrgeo.data.*")
        java_import(jvm, "org.mrgeo.job.*")
        java_import(jvm, "org.mrgeo.mapalgebra.*")
        java_import(jvm, "org.mrgeo.mapalgebra.raster.*")
        java_import(jvm, "org.mrgeo.mapalgebra.vector.*")
        java_import(jvm, "org.mrgeo.utils.*")
        java_import(jvm, "org.mrgeo.utils.logging.*")

    def _initialize(self, pysparkContext=None):
        try:
            if not pysparkContext:
                self._create_gateway(self._host, self._port)
                self._localGateway = True
            else:
                self.gateway = pysparkContext._gateway
                self.gateway_client = pysparkContext._jsc._gateway_client
                self.sparkContext = pysparkContext._jsc.sc()

            self._create_job()

            self._general_imports()
            mapopgenerator.generate(self, self.gateway, self.gateway_client)
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    def _get_jvm(self):
        if not self.gateway:
            self._initialize()

        return self.gateway.jvm

    def _get_job(self):
        if not self._job:
            self._create_job()
        return self._job

    def usedebug(self):
        job = self._get_job()
        job.useDebug()

    def useyarn(self):
        job = self._get_job()
        job.useYarn()

    def start(self):
        if self._started:
            # print("MrGeo is already started")
            return

        jvm = self._get_jvm()
        job = self._get_job()

        job.addMrGeoProperties()
        dpf_properties = jvm.DataProviderFactory.getConfigurationFromProviders()

        for prop in dpf_properties:
            job.setSetting(prop, dpf_properties[prop])

        jvm.DependencyLoader.setPrintMissingDependencies(False)
        jvm.DependencyLoader.resetMissingDependencyList()

        java_gateway.set_field(job, "jars",
                               jvm.StringUtils.concatUnique(
                                   jvm.DependencyLoader.getAndCopyDependencies("org.mrgeo.mapalgebra.MapAlgebra", None),
                                   jvm.DependencyLoader.getAndCopyDependencies(jvm.MapOpFactory.getMapOpClassNames(),
                                                                               None)))

        conf = jvm.MrGeoDriver.prepareJob(job)

        jvm.DependencyLoader.printMissingDependencies()

        if self._localGateway:
            if job.isYarn():
                job.loadYarnSettings()

                # need to override the yarn mode to "yarn-client" for python
                conf.set("spark.master", "yarn-client")

                if not conf.getBoolean("spark.dynamicAllocation.enabled", False):
                    conf.set("spark.executor.instances", str(job.executors()))

                conf.set("spark.executor.cores", str(job.cores()))

                # in yarn-cluster, this is the total memory in the cluster, but here in yarn-client, it is
                # the memory per executor.  Go figure!
                mem = job.executorMemKb()

                overhead = conf.getInt("spark.yarn.executor.memoryOverhead", 384)
                if (mem * 0.1) > overhead:
                    overhead = mem * 0.1

                if overhead < 384:
                    overhead = 384

                mem -= (overhead * 2)  # overhead is 1x for driver and 1x for application master (am)
                conf.set("spark.executor.memory", jvm.SparkUtils.kbtohuman(long(mem), "m"))

            jsc = jvm.JavaSparkContext(conf)
            jsc.setCheckpointDir(jvm.HadoopFileUtils.createJobTmp(jsc.hadoopConfiguration()).toString())
            self.sparkContext = jsc.sc()

        self._started = True
        # print("started")

    def stop(self):
        if self._localGateway:
            if self.sparkContext:
                self.sparkContext.stop()
                self.sparkContext = None
                # self._job = None

        self._started = False

    def disconnect(self):
        if self._started:
            self.stop()

        if self.gateway:
            self.gateway.shutdown()
            self.gateway = None
            self.gateway_client = None
        java_gateway.terminate()

    def list_images(self):
        if not self._started:
            print("ERROR:  You must call start() before list_images()")
            sys.stdout.flush()
            return None

        jvm = self._get_jvm()
        job = self._get_job()

        pstr = job.getSetting(constants.provider_properties, "")
        pp = jvm.ProviderProperties.fromDelimitedString(pstr)

        rawimages = jvm.DataProviderFactory.listImages(pp)

        images = []
        for image in rawimages:
            images.append(str(image))

        return images

    def load_image(self, name):
        if not self._started:
            print("ERROR:  You must call start() before load_image()")
            sys.stdout.flush()
            return None

        jvm = self._get_jvm()
        job = self._get_job()

        pstr = job.getSetting(constants.provider_properties, "")
        pp = jvm.ProviderProperties.fromDelimitedString(pstr)

        dp = jvm.DataProviderFactory.getMrsImageDataProvider(name, jvm.DataProviderFactory.AccessMode.READ, pp)

        mapop = jvm.MrsPyramidMapOp.apply(dp)
        mapop.context(self.sparkContext)

        return RasterMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self._job)

    def ingest_image(self, *names, **kwargs):
        if not self._started:
            print("ERROR:  You must call start() before ingest_images()")
            sys.stdout.flush()
            return None

        jvm = self._get_jvm()
        job = self._get_job()

        zoom = kwargs.pop('zoom', -1)
        skip_preprocessing = kwargs.pop('skip_preprocessing', False)
        nodata_override = kwargs.pop('nodata_override', None)
        categorical = kwargs.pop('categorical', False)
        skip_category_load = kwargs.pop('skip_category_load', False)
        protection_level = kwargs.pop('protection_level', '')

        elements = []
        for arg in names:
            if isinstance(arg, list):
                for a in arg:
                    elements.append(a)
            else:
                elements.append(arg)
        array = self.gateway.new_array(self.gateway.jvm.String, len(elements))
        cnt = 0
        for element in elements:
            array[cnt] = element
            cnt += 1
        mapop = jvm.IngestImageMapOp.createMapOp(array, zoom, skip_preprocessing, nodata_override,
                                                 categorical, skip_category_load, protection_level)

        if (mapop.setup(job, self.sparkContext.getConf()) and
                mapop.execute(self.sparkContext) and
                mapop.teardown(job, self.sparkContext.getConf())):
            return RasterMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=job)
        return None

    def load_vector(self, name):
        if not self._started:
            print("ERROR:  You must call start() before load_vector()")
            sys.stdout.flush()
            return None

        jvm = self._get_jvm()
        job = self._get_job()

        pstr = job.getSetting(constants.provider_properties, "")
        pp = jvm.ProviderProperties.fromDelimitedString(pstr)

        dp = jvm.DataProviderFactory.getVectorDataProvider(name, jvm.DataProviderFactory.AccessMode.READ, pp)

        mapop = jvm.VectorDataMapOp.apply(dp)
        mapop.context(self.sparkContext)

        # print("loaded " + name)

        return VectorMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self._job)

    def create_points(self, coords):
        if not self._started:
            print("ERROR:  You must call start() before create_points()")
            sys.stdout.flush()
            return None

        jvm = self._get_jvm()
        job = self._get_job()

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

        return VectorMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=job)
