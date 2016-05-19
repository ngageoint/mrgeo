from __future__ import print_function

import multiprocessing
import re
from threading import Lock

from py4j.java_gateway import java_import, JavaClass, JavaObject

from pymrgeo import constants

from rastermapop import RasterMapOp
from vectormapop import VectorMapOp

from java_gateway import launch_gateway, set_field, is_remote


class MrGeo(object):
    operators = {"+": ["__add__", "__radd__", "__iadd__"],
                 "-": ["__sub__", "__rsub__", "__isub__"],
                 "*": ["__mul__", "__rmul__", "__imul__"],
                 "/": ["__div__", "__truediv__", "__rdiv__", "__rtruediv__", "__idiv__", "__itruediv__"],
                 "//": [],  # floor div
                 "**": ["__pow__", "__rpow__", "__ipow__"],  # pow
                 "=": [],  # assignment, can't do!
                 "<": ["__lt__"],
                 "<=": ["__le__"],
                 ">": ["__gt__"],
                 ">=": ["__ge__"],
                 "==": ["__eq__"],
                 "!=": ["__ne__"],
                 "<>": [],
                 "!": [],
                 "&&": ["__and__", "__rand__", "__iand__"],
                 "&": [],
                 "||": ["__or__", "__ror__", "__ior__"],
                 "|": [],
                 "~": [],
                 "^": [],
                 "^=": []}
    reserved = ["or", "and", "str", "int", "long", "float", "bool"]

    gateway = None
    lock = Lock()

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
        self.useyarn()

    def initialize(self):

        self._create_job()
        self._load_mapops()

    def _load_mapops(self):
        jvm = self.gateway.jvm
        client = self.gateway._gateway_client
        java_import(jvm, "org.mrgeo.job.*")
        java_import(jvm, "org.mrgeo.mapalgebra.MapOpFactory")
        java_import(jvm, "org.mrgeo.mapalgebra.raster.RasterMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.vector.VectorMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.raster.MrsPyramidMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.IngestImageMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.ExportMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.PointsMapOp")
        java_import(jvm, "org.mrgeo.mapalgebra.MapOp")
        java_import(jvm, "org.mrgeo.utils.SparkUtils")
        java_import(jvm, "org.mrgeo.hdfs.utils.HadoopFileUtils")

        java_import(jvm, "org.mrgeo.data.*")

        mapops = jvm.MapOpFactory.getMapOpClasses()

        for rawmapop in mapops:
            mapop = str(rawmapop.getCanonicalName().rstrip('$'))

            java_import(jvm, mapop)

            cls = JavaClass(mapop, gateway_client=client)

            if self.is_instance_of(cls, jvm.RasterMapOp):
                instance = 'RasterMapOp'
            elif self.is_instance_of(cls, jvm.VectorMapOp):
                instance = 'VectorMapOp'
            elif self.is_instance_of(cls, jvm.MapOp):
                instance = "MapOp"
            else:
                # raise Exception("mapop (" + mapop + ") is not a RasterMapOp, VectorMapOp, or MapOp")
                print("mapop (" + mapop + ") is not a RasterMapOp, VectorMapOp, or MapOp")
                continue

            signatures = jvm.MapOpFactory.getSignatures(mapop)
            # for s in signatures:
            #     print("signature: " + s)

            for method in cls.register():
                codes = None
                if method is not None:
                    name = method.strip().lower()
                    if len(name) > 0:
                        if name in self.reserved:
                            # print("reserved: " + name)
                            continue
                        elif name in self.operators:
                            # print("operator: " + name)
                            codes = self._generate_operator_code(mapop, name, signatures, instance)
                        else:
                            # print("method: " + name)
                            codes = self._generate_method_code(mapop, name, signatures, instance)

                if codes is not None:
                    for method_name, code in codes.iteritems():
                        # print(code)

                        compiled = {}
                        exec code in compiled


                        if instance == 'RasterMapOp':
                            setattr(RasterMapOp, method_name, compiled.get(method_name))
                        elif instance == "VectorMapOp":
                            setattr(VectorMapOp, method_name, compiled.get(method_name))
                        elif self.is_instance_of(cls, jvm.MapOp):
                            setattr(RasterMapOp, method_name, compiled.get(method_name))
                            setattr(VectorMapOp, method_name, compiled.get(method_name))

    def _generate_operator_code(self, mapop, name, signatures, instance):
        methods = self._generate_methods(instance, signatures)

        if len(methods) == 0:
            return None

        # need to change the parameter names to "other" for all except us
        corrected_methods = []
        for method in methods:
            new_method = []
            if len(method) > 2:
                raise Exception("The parameters for an operator can only have 1 or 2 parameters")
            for param in method:
                lst = list(param)
                if lst[1].lower() == 'string' or \
                                lst[1].lower() == 'double' or \
                                lst[1].lower() == 'float' or \
                                lst[1].lower() == 'long' or \
                                lst[1].lower() == 'int' or \
                                lst[1].lower() == 'short' or \
                                lst[1].lower() == 'char' or \
                                lst[1].lower() == 'boolean':
                    lst[0] = "other"
                    lst[2] = "other"
                    # need to add this to the start of the list (in case we eventually check other.mapop from the elif
                elif lst[2] != "self":
                    lst[0] = "other"
                    lst[2] = "other"
                new_method.append(tuple(lst))

            corrected_methods.append(new_method)

        codes = {}
        for method_name in self.operators[name]:
            code = ""

            # Signature
            code += "def " + method_name + "(self, other):" + "\n"
            # code += "    print('" + name + "')\n"

            code += self._generate_imports(mapop)
            code += self._generate_calls(corrected_methods)
            code += self._generate_run()

            codes[method_name] = code
        return codes

    def _generate_method_code(self, mapop, name, signatures, instance):
        methods = self._generate_methods(instance, signatures)

        # print("working on " + name)
        jvm = self.gateway.jvm
        client = self.gateway._gateway_client
        cls = JavaClass(mapop, gateway_client=client)

        is_export = is_remote() and self.is_instance_of(cls, jvm.ExportMapOp)

        if len(methods) == 0:
            return None

        signature = self._generate_signature(methods)

        code = ""
        # Signature
        code += "def " + name + "(" + signature + "):" + "\n"

        # code += "    print('" + name + "')\n"
        code += self._generate_imports(mapop, is_export)
        code += self._generate_calls(methods, is_export)
        code += self._generate_run(is_export)
        # print(code)

        return {name: code}

    def _generate_run(self, is_export=False):
        code = ""

        # Run the MapOp
        code += "    if (op.setup(self.job, self.context.getConf()) and\n"
        code += "        op.execute(self.context) and\n"
        code += "        op.teardown(self.job, self.context.getConf())):\n"
        # copy the Raster/VectorMapOp (so we got all the monkey patched code) and return it as the new mapop
        # TODO:  Add VectorMapOp!
        code += "        new_resource = copy.copy(self)\n"
        code += "        new_resource.mapop = op\n"

        if is_export:
            code += self._generate_saveraster()

        code += "        return new_resource\n"
        code += "    return None\n"
        return code

    def _generate_saveraster(self):
        code = ""
        # code += "        \n"
        code += "        cls = JavaClass('org.mrgeo.mapalgebra.ExportMapOp', gateway_client=self.gateway._gateway_client)\n"
        code += "        if hasattr(self, 'mapop') and self.is_instance_of(self.mapop, 'org.mrgeo.mapalgebra.raster.RasterMapOp') and type(name) is str and isinstance(singleFile, (int, long, float, str)) and isinstance(zoom, (int, long, float)) and isinstance(numTiles, (int, long, float)) and isinstance(mosaic, (int, long, float)) and type(format) is str and isinstance(randomTiles, (int, long, float, str)) and isinstance(tms, (int, long, float, str)) and type(colorscale) is str and type(tileids) is str and type(bounds) is str and isinstance(allLevels, (int, long, float, str)) and isinstance(overridenodata, (int, long, float)):\n"
        code += "            op = cls.create(self.mapop, str(name), True if singleFile else False, int(zoom), int(numTiles), int(mosaic), str(format), True if randomTiles else False, True if tms else False, str(colorscale), str(tileids), str(bounds), True if allLevels else False, float(overridenodata))\n"
        code += "        else:\n"
        code += "            raise Exception('input types differ (TODO: expand this message!)')\n"
        code += "        if (op.setup(self.job, self.context.getConf()) and\n"
        code += "                op.execute(self.context) and\n"
        code += "                op.teardown(self.job, self.context.getConf())):\n"
        code += "            new_resource = copy.copy(self)\n"
        code += "            new_resource.mapop = op\n"
        code += "            gdalutils = JavaClass('org.mrgeo.utils.GDALUtils', gateway_client=self.gateway._gateway_client)\n"
        code += "            java_image = op.image()\n"
        code += "            width = java_image.getRasterXSize()\n"
        code += "            height = java_image.getRasterYSize()\n"
        code += "            options = []\n"
        code += "            if format == 'jpg' or format == 'jpeg':\n"
        code += "                driver_name = 'jpeg'\n"
        code += "                extension = 'jpg'\n"
        code += "            elif format == 'tif' or format == 'tiff' or format == 'geotif' or format == 'geotiff' or format == 'gtif'  or format == 'gtiff':\n"
        code += "                driver_name = 'GTiff'\n"
        code += "                options.append('INTERLEAVE=BAND')\n"
        code += "                options.append('COMPRESS=DEFLATE')\n"
        code += "                options.append('PREDICTOR=1')\n"
        code += "                options.append('ZLEVEL=6')\n"
        code += "                options.append('TILES=YES')\n"
        code += "                if width < 2048:\n"
        code += "                    options.append('BLOCKXSIZE=' + str(width))\n"
        code += "                else:\n"
        code += "                    options.append('BLOCKXSIZE=2048')\n"
        code += "                if height < 2048:\n"
        code += "                    options.append('BLOCKYSIZE=' + str(height))\n"
        code += "                else:\n"
        code += "                    options.append('BLOCKYSIZE=2048')\n"
        code += "                extension = 'tif'\n"
        code += "            else:\n"
        code += "                driver_name = format\n"
        code += "                extension = format\n"
        code += "            datatype = java_image.GetRasterBand(1).getDataType()\n"
        code += "            if not local_name.endswith(extension):\n"
        code += "                local_name += '.' + extension\n"
        code += "            driver = gdal.GetDriverByName(driver_name)\n"
        code += "            local_image = driver.Create(local_name, width, height, java_image.getRasterCount(), datatype, options)\n"
        code += "            local_image.SetProjection(str(java_image.GetProjection()))\n"
        code += "            local_image.SetGeoTransform(java_image.GetGeoTransform())\n"
        code += "            java_nodatas = gdalutils.getnodatas(java_image)\n"
        code += "            print('saving image to ' + local_name)\n"
        code += "            print('downloading data... (' + str(gdalutils.getRasterBytes(java_image, 1) * local_image.RasterCount / 1024) + ' kb uncompressed)')\n"
        code += "            for i in xrange(1, local_image.RasterCount + 1):\n"
        code += "                start = time.time()\n"
        code += "                raw_data = gdalutils.getRasterDataAsCompressedBase64(java_image, i, 0, 0, width, height)\n"
        code += "                print('compressed/encoded data ' + str(len(raw_data)))\n"
        code += "                decoded_data = base64.b64decode(raw_data)\n"
        code += "                print('decoded data ' + str(len(decoded_data)))\n"
        code += "                decompressed_data = zlib.decompress(decoded_data, 16 + zlib.MAX_WBITS)\n"
        code += "                print('decompressed data ' + str(len(decompressed_data)))\n"
        code += "                byte_data = numpy.frombuffer(decompressed_data, dtype='b')\n"
        code += "                print('byte data ' + str(len(byte_data)))\n"
        code += "                image_data = byte_data.view(gdal_array.GDALTypeCodeToNumericTypeCode(datatype))\n"
        code += "                print('gdal-type data ' + str(len(image_data)))\n"
        code += "                image_data = image_data.reshape((-1, width))\n"
        code += "                print('reshaped ' + str(len(image_data)) + ' x ' + str(len(image_data[0])))\n"
        code += "                band = local_image.GetRasterBand(i)\n"
        code += "                print('writing band ' + str(i))\n"
        code += "                band.WriteArray(image_data)\n"
        code += "                end = time.time()\n"
        code += "                print('elapsed time: ' + str(end - start) + ' sec.')\n"
        code += "                band.SetNoDataValue(java_nodatas[i - 1])\n"
        code += "            local_image.FlushCache()\n"
        code += "            print('flushed cache')\n"

        return code

    def _generate_imports(self, mapop, is_export=False):
        code = ""
        # imports
        code += "    import copy\n"
        code += "    from numbers import Number\n"
        if is_export:
            code += "    import base64\n"
            code += "    import numpy\n"
            code += "    from osgeo import gdal, gdal_array\n"
            code += "    import time\n"
            code += "    import zlib\n"

        code += "    from py4j.java_gateway import JavaClass\n"
        # Get the Java class
        code += "    cls = JavaClass('" + mapop + "', gateway_client=self.gateway._gateway_client)\n"
        return code

    def _generate_calls(self, methods, is_export=False):

        # Check the input params and call the appropriate create() method
        firstmethod = True
        varargcode = ""
        code = ""

        if is_export:
            code += "    local_name = name\n"
            code += "    name = 'In-Memory'\n"

        for method in methods:
            iftest = ""
            call = []

            firstparam = True
            for param in method:
                var_name = param[0]
                type_name = param[1]
                call_name = param[2]
                # print("param => " + str(param))
                # print("var name: " + var_name)
                # print("type name: " + type_name)
                # print("call name: " + call_name)

                if param[4]:
                    call_name, it, et, fieldAccessor = self.method_name(type_name, "arg")

                    varargcode += "    for arg in args:\n"
                    varargcode += "        if isinstance(arg, list):\n"
                    varargcode += "            arg_list = arg\n"
                    varargcode += "            for arg in arg_list:\n"
                    varargcode += "                if not(" + it + "):\n"
                    varargcode += "                    raise Exception('input types differ (TODO: expand this message!)')\n"
                    varargcode += "        else:\n"
                    varargcode += "            if not(" + it + "):\n"
                    varargcode += "                raise Exception('input types differ (TODO: expand this message!)')\n"
                    varargcode += "    elements = []\n"
                    varargcode += "    for arg in args:\n"
                    varargcode += "        if isinstance(arg, list):\n"
                    varargcode += "            for a in arg:\n"
                    varargcode += "                elements.append(a" + fieldAccessor + ")\n"
                    varargcode += "        else :\n"
                    varargcode += "            elements.append(arg" + fieldAccessor + ")\n"
                    varargcode += "    array = self.gateway.new_array(self.gateway.jvm." + type_name + ", len(elements))\n"
                    varargcode += "    cnt = 0\n"
                    varargcode += "    for element in elements:\n"
                    varargcode += "        array[cnt] = element\n"
                    varargcode += "        cnt += 1\n"
                    call_name = "array"
                else:
                    if firstparam:
                        firstparam = False
                        if firstmethod:
                            firstmethod = False
                            iftest += "if"
                        else:
                            iftest += "elif"
                    else:
                        iftest += " and"

                    if call_name == "self":
                        var_name = call_name

                    call_name, it, et, fieldAccessor = self.method_name(type_name, var_name)
                    iftest += it

                call += [call_name]

            if len(iftest) > 0:
                iftest += ":\n"
                code += "    " + iftest

            code += "        op = cls.create(" + ", ".join(call) + ')\n'

        code += "    else:\n"
        code += "        raise Exception('input types differ (TODO: expand this message!)')\n"
        # code += "    import inspect\n"
        # code += "    method = inspect.stack()[0][3]\n"
        # code += "    print(method)\n"

        if len(varargcode) > 0:
            code = varargcode + code

        return code

    def method_name(self, type_name, var_name):
        if type_name == "String":
            iftest = " type(" + var_name + ") is str"
            call_name = "str(" + var_name + ")"
            excepttest = "not" + iftest
            fieldAccessor = ""
        elif type_name == "double" or type_name == "float":
            iftest = " isinstance(" + var_name + ", (int, long, float))"
            call_name = "float(" + var_name + ")"
            excepttest = "not" + iftest
            fieldAccessor = ""
        elif type_name == "long":
            iftest = " isinstance(" + var_name + ", (int, long, float))"
            call_name = "long(" + var_name + ")"
            excepttest = "not" + iftest
            fieldAccessor = ""
        elif type_name == "int" or type_name == "Short" or type_name == "Char":
            iftest = " isinstance(" + var_name + ", (int, long, float))"
            call_name = "int(" + var_name + ")"
            excepttest = "not" + iftest
            fieldAccessor = ""
        elif type_name == "boolean":
            iftest = " isinstance(" + var_name + ", (int, long, float, str))"
            call_name = "True if " + var_name + " else False"
            excepttest = "not" + iftest
            fieldAccessor = ""
        elif type_name.endswith("MapOp"):
            base_var = var_name
            var_name += ".mapop"
            iftest = " hasattr(" + base_var + ", 'mapop') and self.is_instance_of(" + var_name + ", '" + type_name + "')"
            call_name = var_name
            excepttest = " hasattr(" + base_var + ", 'mapop') and not self.is_instance_of(" + var_name + ", '" + type_name + "')"
            fieldAccessor = ".mapop"
        else:
            iftest = " self.is_instance_of(" + var_name + ", '" + type_name + "')"
            call_name = var_name
            excepttest = "not" + iftest
            fieldAccessor = ""

        return call_name, iftest, excepttest, fieldAccessor

    def _generate_methods(self, instance, signatures):
        methods = []
        for sig in signatures:
            found = False
            method = []
            for variable in sig.split("|"):
                # print("variable: " + variable)
                names = re.split("[:=]+", variable)
                new_name = names[0]
                new_type = names[1]

                # var args?
                varargs = False
                if new_type.endswith("*"):
                    new_type = new_type[:-1]
                    new_name = "args"
                    varargs = True

                if len(names) == 3:
                    if names[2].lower() == "true":
                        new_value = "True"
                    elif names[2].lower() == "false":
                        new_value = "False"
                    elif names[2].lower() == "infinity":
                        new_value = "float('inf')"
                    elif names[2].lower() == "-infinity":
                        new_value = "float('-inf')"
                    elif names[2].lower() == "null":
                        new_value = "None"
                    else:
                        new_value = names[2]
                else:
                    new_value = None

                if ((not found) and
                        (new_type.endswith("MapOp") or
                             (instance is "RasterMapOp" and new_type.endswith("RasterMapOp")) or
                             (instance is "VectorMapOp" and new_type.endswith("VectorMapOp")))):
                    found = True
                    new_call = "self"
                else:
                    new_call = new_name

                tup = (new_name, new_type, new_call, new_value, varargs)
                method.append(tup)

            methods.append(method)
        return methods

    def _in_signature(self, param, signature):
        for s in signature:
            if s[0] == param[0]:
                if s[1] == param[1]:
                    if s[3] == param[3]:
                        return True
                    else:
                        raise Exception("only default values differ: " + str(s) + ": " + str(param))
                else:
                    raise Exception("type parameters differ: " + s[1] + ": " + param[1])
                #                    raise Exception("type parameters differ: " + str(s) + ": " + str(param))
        return False

    def _generate_signature(self, methods):
        signature = []
        dual = len(methods) > 1
        for method in methods:
            for param in method:
                # print("Param: " + str(param))
                if not param[2] == "self" and not self._in_signature(param, signature):
                    signature.append(param)
                    if param[4]:
                        # var args must be the last parameter
                        break

        sig = ["self"]
        for s in signature:
            if s[4]:
                sig += ["*args"]
            else:
                if s[3] is not None:
                    if (s[3].endswith("NaN")):
                        sig += [s[0] + "=float('nan')"]
                    else:
                        sig += [s[0] + "=" + s[3]]
                elif dual:
                    sig += [s[0] + "=None"]
                else:
                    sig += [s[0]]

        return ",".join(sig)

    # @staticmethod
    # def _generate_code(mapop, name, signatures, instance):
    #
    #     signature, call, types, values = MrGeo._generate_params(instance, signatures)
    #
    #     sig = ""
    #     for s, d in zip(signature, values):
    #         if len(sig) > 0:
    #             sig += ", "
    #         sig += s
    #         if d is not None:
    #             sig += "=" + str(d)
    #
    #     code = ""
    #     code += "def " + name + "(" + sig + "):" + "\n"
    #     code += "    from py4j.java_gateway import JavaClass\n"
    #     code += "    #from rastermapop import RasterMapOp\n"
    #     code += "    import copy\n"
    #     code += "    print('" + name + "')\n"
    #     code += "    cls = JavaClass('" + mapop + "', gateway_client=self.gateway._gateway_client)\n"
    #     code += "    newop = cls.apply(" + ", ".join(call) + ')\n'
    #     code += "    if (newop.setup(self.job, self.context.getConf()) and\n"
    #     code += "        newop.execute(self.context) and\n"
    #     code += "        newop.teardown(self.job, self.context.getConf())):\n"
    #     code += "        new_raster = copy.copy(self)\n"
    #     code += "        new_raster.mapop = newop\n"
    #     code += "        return new_raster\n"
    #     code += "    return None\n"
    #
    #     # print(code)
    #
    #     return code

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
        elif isinstance(java_object, JavaObject):
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

    def usedebug(self):
        self.job.useDebug()

    def useyarn(self):
        self.job.useYarn()

    def start(self):
        jvm = self.gateway.jvm

        job = self.job

        job.addMrGeoProperties()
        dpf_properties = jvm.DataProviderFactory.getConfigurationFromProviders()

        for prop in dpf_properties:
            job.setSetting(prop, dpf_properties[prop])

        if job.isDebug():
            master = "local"
        elif job.isSpark():
            # TODO:  get the master for spark
            master = ""
        elif job.isYarn():
            master = "yarn-client"
            job.loadYarnSettings()
        else:
            cpus = (multiprocessing.cpu_count() / 4) * 3
            if cpus < 2:
                master = "local"
            else:
                master = "local[" + str(cpus) + "]"

        set_field(job, "jars",
                  jvm.StringUtils.concatUnique(
                      jvm.DependencyLoader.getAndCopyDependencies("org.mrgeo.mapalgebra.MapAlgebra", None),
                      jvm.DependencyLoader.getAndCopyDependencies(jvm.MapOpFactory.getMapOpClassNames(), None)))

        conf = jvm.MrGeoDriver.prepareJob(job)

        # need to override the yarn mode to "yarn-client" for python
        if job.isYarn():
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

            # conf.set("spark.yarn.am.cores", str(1))
            # conf.set("spark.driver.memory", jvm.SparkUtils.kbtohuman(job.executorMemKb(), "m"))

            # print('driver mem: ' + conf.get("spark.driver.memory")) #  + ' cores: ' + conf.get("spark.driver.cores"))
            # print('executor mem: ' + conf.get("spark.executor.memory") + ' cores: ' + conf.get("spark.executor.cores"))

        jsc = jvm.JavaSparkContext(conf)
        jsc.setCheckpointDir(jvm.HadoopFileUtils.createJobTmp(jsc.hadoopConfiguration()).toString())
        self.sparkContext = jsc.sc()

        # print("started")

    def stop(self):
        if self.sparkContext:
            self.sparkContext.stop()
            self.sparkContext = None

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

        # Convert from a python list to a Java array
        cnt = 0
        array = self.gateway.new_array(self.gateway.jvm.double, len(coords))
        for coord in coords:
            array[cnt] = coord
            cnt += 1

        mapop = jvm.PointsMapOp.apply(array)
        mapop.context(self.sparkContext)

        return VectorMapOp(mapop=mapop, gateway=self.gateway, context=self.sparkContext, job=self.job)
