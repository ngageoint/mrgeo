from __future__ import print_function

import imp
import multiprocessing
import os
import re
import sys
from threading import Lock

from py4j.java_gateway import java_import, JavaClass, JavaObject

from pymrgeo import constants
from pyspark.context import SparkContext

from rastermapop import RasterMapOp

from java_gateway import launch_gateway, get_field, set_field


class MrGeo(object):
    gateway = None
    lock = Lock()

    sparkPyContext = None
    sparkContext = None
    job = None

    def __init__(self, gateway=None):

        MrGeo.ensure_gateway_initialized(self, gateway=gateway)
        try:
            self.initialize()
        except:
            # If an error occurs, clean up in order to allow future SparkContext creation:
            self.stop()
            raise

    @classmethod
    def ensure_gateway_initialized(cls, instance=None, gateway=None):
        """
        Checks whether a SparkContext is initialized or not.
        Throws error if a SparkContext is already running.
        """
        with MrGeo.lock:
            if not MrGeo.gateway:
                MrGeo.gateway = gateway or launch_gateway()
                MrGeo.jvm = MrGeo.gateway.jvm

    def _create_job(self):
        jvm = self.gateway.jvm
        java_import(jvm, "org.mrgeo.data.DataProviderFactory")
        java_import(jvm, "org.mrgeo.job.*")
        java_import(jvm, "org.mrgeo.utils.DependencyLoader")
        java_import(jvm, "org.mrgeo.utils.StringUtils")

        appname = "PyMrGeo"

        self.job = jvm.JobArguments()
        set_field(self.job, "name", appname)

        # Yarn in the default
        self.useYarn()

    def initialize(self):
        self._create_job()
        self._load_mapops()

    def _load_mapops(self):

        symbols = {"+", "-", "*", "/",
                   "=", "<", "<=", ">", ">=", "==", "!=", "<>", "!",
                   "&&", "&", "||", "|", "^", "^="}
        reserved = {"or", "and"}

        jvm = self.gateway.jvm
        client = self.gateway._gateway_client
        java_import(jvm, "org.mrgeo.job.*")
        java_import(jvm, "org.mrgeo.mapalgebra.MapOpFactory")
        java_import(jvm, "org.mrgeo.mapalgebra.raster.RasterMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.raster.MrsPyramidMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.vector.VectorMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.MapOp")
        java_import(jvm, "org.mrgeo.utils.SparkUtils")

        #java_import(jvm, "org.mrgeo.data.DataProviderFactory")
        #java_import(jvm, "org.mrgeo.data.DataProviderFactory.AccessMode")
        java_import(jvm, "org.mrgeo.data.*")

        mapops = jvm.MapOpFactory.getMapOpClasses()

        # print("MapOps")
        for rawmapop in mapops:
            mapop = rawmapop.getCanonicalName().rstrip('$')

            signatures = jvm.MapOpFactory.getSignatures(str(mapop))

            # print("  " + mapop)
            java_import(jvm, mapop)

            cls = JavaClass(str(mapop), gateway_client=client)

            if self.is_instance_of(cls, jvm.RasterMapOp):
                instance = 'RasterMapOp'
            elif self.is_instance_of(cls, jvm.VectorMapOp):
                instance = 'VectorMapOp'
            elif self.is_instance_of(cls, jvm.MapOp):
                instance = "MapOp"
            else:
                #raise Exception("mapop (" + mapop + ") is not a RasterMapOp, VectorMapOp, or MapOp")
                print("mapop (" + mapop + ") is not a RasterMapOp, VectorMapOp, or MapOp")
                break

            names = {}
            returntype = cls.returnType()
            for method in cls.register():
                if method is not None:
                    name = method.strip()
                    if len(name) > 0:
                        if name in reserved:
                            print("reserved: " + name)
                        elif name in symbols:
                            print("symbol: " + name)
                        else:
                            print("method: " + name)

                            signature, call, types, values = self._generate_signature(instance, signatures)
                            if name in names:
                                existing_sig, existing_call, existing_types, existing_defs = names[name]
                                signature, call, types, values = \
                                    MrGeo._merge_signatures(existing_sig, existing_call, existing_types,
                                                            existing_defs, signature, call, types, values)

                                names[name] = (signature, call, types, values)

            for name in names:
                call, signature, defaults = names[name]
                code = MrGeo._generate_code(name, mapop, signature, call, defaults, returntype)

                compiled = {}
                exec code in compiled

                if instance == 'RasterMapOp':
                    setattr(RasterMapOp, name, compiled.get(name))
                elif instance == "VectorMapOp":
                    #setattr(VectorMapOp, name, compiled.get(name))
                    pass
                elif self.is_instance_of(cls, jvm.MapOp):
                    setattr(RasterMapOp, name, compiled.get(name))
                    #setattr(VectorMapOp, name, compiled.get(name))

    @staticmethod
    def _merge_signatures(existing_sig, existing_call, existing_types, existing_defs,
                          new_signature, new_call, new_types, new_values):

        call = existing_call
        signature = existing_sig
        defaults = existing_defs
        types = existing_types

        return signature, call, types, defaults

    @staticmethod
    def _generate_signature(instance, signatures):
        signature = ["self"]
        call = []
        types = []
        values = []

        found = False
        for sig in signatures:
            for variable in sig.split(","):
                names = re.split("[:=]+", variable)
                name = names[0]
                typ = names[1]

                if len(names) == 3:
                    value = names[2]
                else:
                    value = None

                if ((not found) and
                        (typ.endswith("MapOp") or
                             (instance is "RasterMapOp" and typ.endswith("RasterMapOp")) or
                             (instance is "VectorMapOp" and typ.endswith("VectorMapOp")))):
                    found = True
                    call += ["self.mapop"]
                else:
                    call += [name]

                    s = name
                    if value is not None:
                        s += "=" + value
                    signature += [s]

                values += [value]
                types += [typ]

        return signature, call, types, values

    @staticmethod
    def _generate_code(name, mapop, signature, call, defaults, returntype):

        sig = ""
        for s, d in zip(signature, defaults):
            if len(sig) > 0:
                sig += ", "
            sig += s
            if d is not None:
                sig += "=" + str(d)

        code = "def " + name + "(" + sig + "):" + "\n"
        code += "    from py4j.java_gateway import JavaClass\n"
        code += "    #from rastermapop import RasterMapOp\n"
        code += "    import copy\n"
        code += "    print('" + name + "')\n"
        code += "    cls = JavaClass('" + mapop + "', gateway_client=self.gateway._gateway_client)\n"
        code += "    newop = cls.apply(" + ", ".join(call) + ')\n'
        code += "    if (newop.setup(self.job, self.context.getConf()) and\n"
        code += "        newop.execute(self.context) and\n"
        code += "        newop.teardown(self.job, self.context.getConf())):\n"
        code += "        new_raster = copy.copy(self)\n"
        code += "        new_raster.mapop = newop\n"
        code += "        return new_raster\n"
        code += "        #return " + returntype + "(mapop = newop, gateway=self.gateway, context=self.context, job=self.job)\n"
        code += "    return None\n"

        # print(code)

        return code

    def is_instance_of(self, java_object, java_class):
        if isinstance(java_class, basestring):
            name = java_class
        elif isinstance(java_class, JavaClass):
            name = java_class._fqn
        elif isinstance(java_class, JavaObject):
            name = java_class.getClass()
        else:
            raise Exception("java_class must be a string, a JavaClass, or a JavaObject")

        jvm = self.gateway.jvm
        name = jvm.Class.forName(name).getCanonicalName()

        if isinstance(java_object, JavaClass):
            cls = jvm.Class.forName(java_object._fqn)
        elif isinstance(java_class, JavaObject):
            cls = java_object.getClass()
        else:
            raise Exception("java_object must be a JavaClass, or a JavaObject")

        if cls.getCanonicalName() == name:
            return True

        return self._is_instance_of(cls.getSuperclass(), name)

    def _is_instance_of(self, clazz, name):

        if clazz:
            if clazz.getCanonicalName() == name:
                return True

            return self._is_instance_of(clazz.getSuperclass(), name)

        return False

    def useDebug(self):
        self.job.useDebug()

    def useYarn(self):
        self.job.useYarn()

    def start(self):
        jvm = self.gateway.jvm
        self.job.addMrGeoProperties()
        dpf_properties = jvm.DataProviderFactory.getConfigurationFromProviders()

        for prop in dpf_properties:
            self.job.setSetting(prop, dpf_properties[prop])

        if self.job.isDebug():
            master = "local"
        elif self.job.isSpark():
            #TODO:  get the master for spark
            master = ""
        elif self.job.isYarn():
            master = "yarn-client"
        else:
            cpus = (multiprocessing.cpu_count() / 4) * 3
            if cpus < 2:
                master = "local"
            else:
                master = "local[" + str(cpus) + "]"

        set_field(self.job, "jars", jvm.StringUtils.concatUnique(jvm.DependencyLoader.getAndCopyDependencies("org.mrgeo.mapalgebra.MapAlgebra", None),
                                                                 jvm.DependencyLoader.getAndCopyDependencies(jvm.MapOpFactory.getMapOpClassNames(), None)))

        conf = jvm.PrepareJob.prepareJob(self.job)

        # need to override the yarn mode to "yarn-client" for python
        if self.job.isYarn():
            conf.set("spark.master", "yarn-client")

            mem = jvm.SparkUtils.humantokb(conf.get("spark.executor.memory"))
            workers = int(conf.get("spark.executor.instances")) + 1  # one for the driver

            conf.set("spark.executor.memory", jvm.SparkUtils.kbtohuman(long(mem / workers), "m"))

        for a in conf.getAll():
            print(a._1(), a._2())

        # jsc = jvm.JavaSparkContext(master, appName, sparkHome, jars)
        jsc = jvm.JavaSparkContext(conf)
        self.sparkContext = jsc.sc()
        self.sparkPyContext = SparkContext(master=master, appName=self.job.name(), jsc=jsc, gateway=self.gateway)

        print("started")

    def stop(self):
        if self.sparkContext:
            self.sparkContext.stop()
            self.sparkContext = None

        if self.sparkPyContext:
            self.sparkPyContext.stop()
            self.sparkPyContext = None

        self.job = None

    def load_resource(self, name):
        jvm = self.gateway.jvm

        #providerProperties = ProviderProperties.fromDelimitedString(job.getSetting(MapAlgebra.ProviderProperties, ""))

        pstr = self.job.getSetting(constants.provider_properties, "")
        pp = jvm.ProviderProperties.fromDelimitedString(pstr)

        dp = jvm.DataProviderFactory.getMrsImageDataProvider(name, jvm.DataProviderFactory.AccessMode.READ, pp)

        mapop = jvm.MrsPyramidMapOp.apply(dp)
        mapop.context(self.sparkContext)

        print("loaded " + name)

        return RasterMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self.job)
