from __future__ import print_function

import re
import sys
import traceback

from py4j.java_gateway import JavaClass, java_import

from pymrgeo.code_generator import CodeGenerator
from pymrgeo.instance import is_instance_of
from pymrgeo.java_gateway import is_remote
from pymrgeo.rastermapop import RasterMapOp
from pymrgeo.vectormapop import VectorMapOp

# Some MrGeo map ops include operators as their function name. Each
# of those operators work in command-line map algebra. This data
# structure determines how/if those operators are overloaded in
# Python for MrGeo.
#
# The key is the name/symbol of the operator. The value is an array
# of two elements, the first being an array of python "magic methods"
# to map the operator to when the left-hand operand is "self" (e.g. an
# image or vector map op). The second element is an array of python
# "magic methods" to map to the operator when the right-hand operand
# is "self" (e.g. "2 ** image"). For operators where operand ordering
# does not matter (e.g. +, *, etc...) or there is only one operand,
# the second element is an empty array. For operators that cannot be
# overridden in python, both elements of the array are empty arrays.
#
# A scenario where this is important is with the expression "image ** 2"
# compared to "2 ** image". In the first case, python will invoke the
# __pow__  magic mathod on the image object (so image is "self" when the
# method is called. In the second case, python will invoke __rand__ again
# where "self" is image. However, in that case, our overridden method
# needs to invoke map algebra on the Java/Scala side with the arguments
# reversed.
_operators = {"+": [["__add__", "__iadd__", "__radd__"], []],
              "-": [["__sub__", "__isub__"], ["__rsub__"]],
              "*": [["__mul__", "__imul__", "__rmul__"], []],
              "/": [["__div__", "__truediv__", "__idiv__", "__itruediv__"], ["__rdiv__", "__rtruediv__"]],
              "//": [[], []],  # floor div
              "**": [["__pow__", "__ipow__"], ["__rpow__"]],  # pow
              "=": [],  # assignment, can't do!
              "<": [["__lt__"], []],
              "<=": [["__le__"], []],
              ">": [["__gt__"], []],
              ">=": [["__ge__"], []],
              "==": [["__eq__"], []],
              "!=": [["__ne__"], []],
              "<>": [[], []],
              "!": [[], []],
              "&&": [[], []],  # can't override logical and
              "&": [["__and__", "__iand__", "__rand__"], []],
              "||": [[], []],  # can't override logical or
              "|": [["__or__", "__ior__", "__ror__"], []],
              "~": [["__invert__"], []],
              "^": [["__xor__", "__ixor__", "__rxor__"], []],
              "^=": [[], []]}
_reserved = ["or", "and", "str", "int", "long", "float", "bool"]

_mapop_code = {}
_rastermapop_code = {}
_vectormapop_code = {}

_initialized = False


def _exceptionhook(ex_cls, ex, tb, method_name=None):
    stack = traceback.extract_tb(tb)
    print(ex_cls.__name__ + ' (' + str(ex) + ')', file=sys.stderr)
    for st in stack:
        file = st[0]
        line = st[1]
        method = st[2]
        srccode = st[3]
        cls = None
        code = None
        if file == method + '.py':
            if _rastermapop_code.has_key(method):
                code = _rastermapop_code[method]
                cls = 'RasterMapOp'
            elif _vectormapop_code.has_key(method):
                code = _vectormapop_code[method]
                cls = 'VectorMapOp'
            elif _mapop_code.has_key(method):
                code = _mapop_code[method]
                cls = 'MapOp'
            else:
                pass

        if code:
            print('  File <' + cls + '.internal>, line ' +
                  str(line) + ', in ' + cls + '.' + method.strip(), file=sys.stderr)

            srccode = code.generate().split('\n')
            cnt = 1
            for c in srccode:
                if cnt == line:
                    print('==> ' + c + ' <==', file=sys.stderr)
                else:
                    print('    ' + c, file=sys.stderr)
                cnt += 1
        else:
            print('  File "' + file.strip() + '", line ' +
                  str(line) + ', in ' + method.strip(), file=sys.stderr)
            print('    ' + srccode.strip() if srccode else "<unknown file>", file=sys.stderr)

            print(''.join(traceback.format_tb(tb)))
            print('{0}: {1}'.format(ex_cls, ex))


# Always setup the hook
sys.excepthook = _exceptionhook


def generate(mrgeo, gateway, gateway_client):
    global _initialized

    if _initialized:
        return

    jvm = gateway.jvm
    client = gateway_client
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

        # Skip IngestImageMapOp because there is an explicit method defined in
        # MrGeo class for ingesting an image, and _get_instance_type will raise
        # an exception when run against that map op.
        if not mapop.endswith(".IngestImageMapOp") and not mapop.endswith(".InlineCsvMapOp"):
            java_import(jvm, mapop)

            cls = JavaClass(mapop, gateway_client=client)

            signatures = jvm.MapOpFactory.getSignatures(mapop)
            instance = _get_instance_type(signatures, gateway, cls, mapop)
            # for s in signatures:
            #     print("signature: " + s)

            for method in cls.register():
                ooCodes = None
                procCodes = None
                if method is not None:
                    name = method.strip().lower()
                    if len(name) > 0:
                        if name in _reserved:
                            # print("reserved: " + name)
                            continue
                        elif name in _operators:
                            # print("operator: " + name)
                            ooCodes = _generate_operator_code(mapop, name, signatures, instance)
                        else:
                            # print("method: " + name)
                            ooCodes = _generate_oo_method_code(gateway, client, mapop, name, signatures, instance)
                            procCodes = _generate_procedural_method_code(gateway, client, mapop, name, signatures,
                                                                         instance)

                if ooCodes is not None:
                    for method_name, code in ooCodes.items():
                        # if method_name == "rasterizevector":
                        #    print(code.generate())

                        if instance == 'RasterMapOp':
                            _rastermapop_code[method_name] = code
                            setattr(RasterMapOp, method_name, code.compile(method_name).get(method_name))
                        elif instance == "VectorMapOp":
                            _vectormapop_code[method_name] = code
                            setattr(VectorMapOp, method_name, code.compile(method_name).get(method_name))
                        elif is_instance_of(gateway, cls, jvm.MapOp):
                            _mapop_code[method_name] = code
                            setattr(RasterMapOp, method_name, code.compile(method_name).get(method_name))
                            setattr(VectorMapOp, method_name, code.compile(method_name).get(method_name))
                if procCodes is not None:
                    for method_name, code in procCodes.items():
                        _mapop_code[method_name] = code
                        setattr(mrgeo, method_name, code.compile(method_name).get(method_name))

    _initialized = True


def _get_instance_type(signatures, gateway, cls, mapop):
    type_map = {'RasterMapOp': 0, 'VectorMapOp': 0, 'MapOp': 0}
    for sig in signatures:
        has_type = {'RasterMapOp': False, 'VectorMapOp': False, 'MapOp': False}
        for variable in sig.split("|"):
            # print("variable: " + variable)
            names = re.split("[:=]+", variable)
            new_type = names[1][names[1].rfind('.') + 1:]

            if new_type.endswith("*"):
                new_type = new_type[:-1]

            if new_type == 'RasterMapOp' or new_type == 'VectorMapOp' or new_type == 'MapOp':
                has_type[new_type] = True
        for t, v in has_type.iteritems():
            if v:
                type_map[t] += 1

    # Make sure that all of the signatures have an argument of one of the map op types.
    # If the map op is either RasterMapOp or VectorMapOp, and all of the signatures have
    # an argument of that type, then use that for the instance type.
    if is_instance_of(gateway, cls, 'org.mrgeo.mapalgebra.raster.RasterMapOp'):
        if type_map['RasterMapOp'] == len(signatures):
            return 'RasterMapOp'
    elif is_instance_of(gateway, cls, 'org.mrgeo.mapalgebra.vector.VectorMapOp'):
        if type_map['VectorMapOp'] == len(signatures):
            return 'VectorMapOp'
    # There is at least one signature that does not include a parameter of the same type
    # as the map op itself. Instead, we get a type that is represented in all the signatures.
    for t, v in type_map.iteritems():
        if v == len(signatures):
            return t

    msg = 'Cannot determine an instance type to use for ' + mapop
    print(msg)
    raise Exception(msg)


def _generate_operator_code(mapop, name, signatures, instance):
    methods = _generate_methods(instance, signatures)

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
    for op_index in range(0, 2):
        for mname in _operators[name][op_index]:
            # print("Processing " + mname)
            generator = CodeGenerator()

            # Signature
            if len(corrected_methods) == 1:
                generator.write("def " + mname + "(self):", post_indent=True)
            else:
                generator.write("def " + mname + "(self, other):", post_indent=True)
            # code += "    print('" + name + "')\n"

            _generate_imports(generator, mapop)
            _generate_calls(generator, corrected_methods, is_reverse=True if op_index == 1 else False)
            _generate_run(generator, instance)

            codes[mname] = generator
    return codes


def _generate_oo_method_code(gateway, client, mapop, name, signatures, instance):
    methods = _generate_methods(instance, signatures)

    # print("working on " + name)
    jvm = gateway.jvm
    cls = JavaClass(mapop, gateway_client=client)

    is_export = is_remote() and is_instance_of(gateway, cls, jvm.ExportMapOp)

    if len(methods) == 0:
        return None

    signature = _generate_oo_signature(methods)

    generator = CodeGenerator()
    # Signature
    generator.write("def " + name + "(" + signature + "):", post_indent=True)

    # code += "    print('" + name + "')\n"
    _generate_imports(generator, mapop, is_export)
    _generate_calls(generator, methods, is_export=is_export)
    _generate_run(generator, instance, is_export)
    # print(code)

    return {name: generator}


def _generate_procedural_method_code(gateway, client, mapop, name, signatures, instance):
    methods = _generate_methods(instance, signatures)

    # print("working on " + name)
    jvm = gateway.jvm
    cls = JavaClass(mapop, gateway_client=client)

    is_export = is_remote() and is_instance_of(gateway, cls, jvm.ExportMapOp)

    if len(methods) == 0:
        return None

    signature, self_method = _generate_proc_signature(methods)

    generator = CodeGenerator()
    # Signature
    generator.write("def " + name + "(" + signature + "):", post_indent=True)

    # code += "    print('" + name + "')\n"
    _generate_imports(generator, mapop, is_export)
    _generate_calls(generator, methods, is_export=is_export)
    _generate_run(generator, instance, is_export)
    # print(code)

    code = generator.generate()
    code = code.replace("self", self_method)

    generator.begin()
    for line in code.split("\n"):
        generator.write(line)
    return {name: generator}


def _generate_run(generator, instance, is_export=False):
    if is_export:
        ex_generator = _generate_saveraster()
        generator.append(ex_generator)
        generator.force_level(generator.get_level() + ex_generator.get_level())
    else:
        # Run the MapOp
        generator.write("if (op.setup(self.job, self.context.getConf()) and", post_indent=True)
        generator.write("op.execute(self.context) and")
        generator.write("op.teardown(self.job, self.context.getConf())):")
        # Return a new python RasterMapOp or VectorMapOp to wrap the Java MapOp
        generator.write("if is_instance_of(self.gateway, op, 'org.mrgeo.mapalgebra.raster.RasterMapOp'):",
                        post_indent=True)
        generator.write(
            "new_resource = RasterMapOp(gateway=self.gateway, context=self.context, mapop=op, job=self.job)",
            post_unindent=True)
        generator.write("elif is_instance_of(self.gateway, op, 'org.mrgeo.mapalgebra.vector.VectorMapOp'):",
                        post_indent=True)
        generator.write(
            "new_resource = VectorMapOp(gateway=self.gateway, context=self.context, mapop=op, job=self.job)",
            post_unindent=True)
        generator.write("else:", post_indent=True)
        generator.write("raise Exception('Unable to wrap a python object around returned map op: ' + str(op))",
                        post_unindent=True)

    generator.write("return new_resource", post_unindent=True)
    generator.write("return None")
    return generator


def _generate_saveraster():
    generator = CodeGenerator()

    generator.write("cls = JavaClass('org.mrgeo.mapalgebra.ExportMapOp', gateway_client=self.gateway._gateway_client)")
    generator.write(
        "if hasattr(self, 'mapop') and self.is_instance_of(self.mapop, 'org.mrgeo.mapalgebra.raster.RasterMapOp') and type(name) is str and isinstance(singleFile, (int, long, float, str)) and isinstance(zoom, (int, long, float)) and isinstance(numTiles, (int, long, float)) and isinstance(mosaic, (int, long, float)) and type(format) is str and isinstance(randomTiles, (int, long, float, str)) and isinstance(tms, (int, long, float, str)) and type(colorscale) is str and type(tileids) is str and type(bounds) is str and isinstance(allLevels, (int, long, float, str)) and isinstance(overridenodata, (int, long, float)):",
        post_indent=True)
    generator.write(
        "op = cls.create(self.mapop, str(name), True if singleFile else False, int(zoom), int(numTiles), int(mosaic), str(format), True if randomTiles else False, True if tms else False, str(colorscale), str(tileids), str(bounds), True if allLevels else False, float(overridenodata))",
        post_unindent=True)
    generator.write("else:", post_indent=True)
    generator.write("raise Exception('input types differ (TODO: expand this message!)')", post_unindent=True)
    generator.write("if (op.setup(self.job, self.context.getConf()) and", post_indent=True)
    generator.write("op.execute(self.context) and")
    generator.write("op.teardown(self.job, self.context.getConf())):")
    generator.write("if is_instance_of(self.gateway, op, 'org.mrgeo.mapalgebra.raster.RasterMapOp'):", post_indent=True)
    generator.write("new_resource = RasterMapOp(gateway=self.gateway, context=self.context, mapop=op, job=self.job)",
                    post_unindent=True)
    generator.write("elif is_instance_of(self.gateway, op, 'org.mrgeo.mapalgebra.vector.VectorMapOp'):",
                    post_indent=True)
    generator.write("new_resource = VectorMapOp(gateway=self.gateway, context=self.context, mapop=op, job=self.job)",
                    post_unindent=True)
    generator.write("else:", post_indent=True)
    generator.write("raise Exception('Unable to wrap a python object around returned map op: ' + str(op))",
                    post_unindent=True)
    generator.write("gdalutils = JavaClass('org.mrgeo.utils.GDALUtils', gateway_client=self.gateway._gateway_client)")
    generator.write("java_image = op.image()")
    generator.write("width = java_image.getRasterXSize()")
    generator.write("height = java_image.getRasterYSize()")
    generator.write("options = []")
    generator.write("if format == 'jpg' or format == 'jpeg':", post_indent=True)
    generator.write("driver_name = 'jpeg'")
    generator.write("extension = 'jpg'", post_unindent=True)
    generator.write(
        "elif format == 'tif' or format == 'tiff' or format == 'geotif' or format == 'geotiff' or format == 'gtif'  or format == 'gtiff':",
        post_indent=True)
    generator.write("driver_name = 'GTiff'")
    generator.write("options.append('INTERLEAVE=BAND')")
    generator.write("options.append('COMPRESS=DEFLATE')")
    generator.write("options.append('PREDICTOR=1')")
    generator.write("options.append('ZLEVEL=6')")
    generator.write("options.append('TILES=YES')")
    generator.write("if width < 2048:", post_indent=True)
    generator.write("options.append('BLOCKXSIZE=' + str(width))", post_unindent=True)
    generator.write("else:", post_indent=True)
    generator.write("options.append('BLOCKXSIZE=2048')", post_unindent=True)
    generator.write("if height < 2048:", post_indent=True)
    generator.write("options.append('BLOCKYSIZE=' + str(height))", post_unindent=True)
    generator.write("else:", post_indent=True)
    generator.write("options.append('BLOCKYSIZE=2048')", post_unindent=True)
    generator.write("extension = 'tif'", post_unindent=True)
    generator.write("else:", post_indent=True)
    generator.write("driver_name = format")
    generator.write("extension = format", post_unindent=True)
    generator.write("datatype = java_image.GetRasterBand(1).getDataType()")
    generator.write("if not local_name.endswith(extension):", post_indent=True)
    generator.write("local_name += '.' + extension", post_unindent=True)
    generator.write("driver = gdal.GetDriverByName(driver_name)")
    generator.write(
        "local_image = driver.Create(local_name, width, height, java_image.getRasterCount(), datatype, options)")
    generator.write("local_image.SetProjection(str(java_image.GetProjection()))")
    generator.write("local_image.SetGeoTransform(java_image.GetGeoTransform())")
    generator.write("java_nodatas = gdalutils.getnodatas(java_image)")
    generator.write("print('saving image to ' + local_name)")
    generator.write(
        "print('downloading data... (' + str(gdalutils.getRasterBytes(java_image, 1) * local_image.RasterCount / 1024) + ' kb uncompressed)')")
    generator.write("for i in xrange(1, local_image.RasterCount + 1):", post_indent=True)
    generator.write("start = time.time()")
    generator.write("raw_data = gdalutils.getRasterDataAsCompressedBase64(java_image, i, 0, 0, width, height)")
    generator.write("print('compressed/encoded data ' + str(len(raw_data)))")
    generator.write("decoded_data = base64.b64decode(raw_data)")
    generator.write("print('decoded data ' + str(len(decoded_data)))")
    generator.write("decompressed_data = zlib.decompress(decoded_data, 16 + zlib.MAX_WBITS)")
    generator.write("print('decompressed data ' + str(len(decompressed_data)))")
    generator.write("byte_data = numpy.frombuffer(decompressed_data, dtype='b')")
    generator.write("print('byte data ' + str(len(byte_data)))")
    generator.write("image_data = byte_data.view(gdal_array.GDALTypeCodeToNumericTypeCode(datatype))")
    generator.write("print('gdal-type data ' + str(len(image_data)))")
    generator.write("image_data = image_data.reshape((-1, width))")
    generator.write("print('reshaped ' + str(len(image_data)) + ' x ' + str(len(image_data[0])))")
    generator.write("band = local_image.GetRasterBand(i)")
    generator.write("print('writing band ' + str(i))")
    generator.write("band.WriteArray(image_data)")
    generator.write("end = time.time()")
    generator.write("print('elapsed time: ' + str(end - start) + ' sec.')")
    generator.write("band.SetNoDataValue(java_nodatas[i - 1])", post_unindent=True)
    generator.write("local_image.FlushCache()")

    return generator


def _generate_imports(generator, mapop, is_export=False):
    # imports
    generator.write("from pymrgeo.instance import is_instance_of")
    generator.write("from pymrgeo import RasterMapOp")
    generator.write("from pymrgeo import VectorMapOp")
    generator.write("from numbers import Number")
    if is_export:
        generator.write("import base64")
        generator.write("import numpy")
        generator.write("from osgeo import gdal, gdal_array")
        generator.write("import time")
        generator.write("import zlib")

    generator.write("from py4j.java_gateway import JavaClass")
    # Get the Java class
    generator.write("cls = JavaClass('" + mapop + "', gateway_client=self.gateway._gateway_client)")


def _generate_calls(generator, methods, is_export=False, is_reverse=False):
    # Check the input params and call the appropriate create() method
    firstmethod = True
    varargcode = CodeGenerator()

    if is_export:
        generator.write("local_name = name")
        generator.write("name = 'In-Memory'")

    for method in methods:
        iftest = ""
        call = []

        firstparam = True
        for param in method:
            var_name = param[0]
            type_name = param[1]
            call_name = param[2]
            default_value = param[3]
            # print("param => " + str(param))
            # print("var name: " + var_name)
            # print("type name: " + type_name)
            # print("call name: " + call_name)

            if param[4]:
                call_name, it, et, accessor = _method_name(type_name, "arg", None)

                varargcode.write("for arg in args:", post_indent=True)
                varargcode.write("if isinstance(arg, list):", post_indent=True)
                varargcode.write("arg_list = arg")
                varargcode.write("for arg in arg_list:", post_indent=True)
                varargcode.write("if not(" + it + "):", post_indent=True)
                varargcode.write("raise Exception('input types differ (TODO: expand this message!)')")
                varargcode.unindent(3)
                varargcode.write("else:", post_indent=True)
                varargcode.write("if not(" + it + "):", post_indent=True)
                varargcode.write("raise Exception('input types differ (TODO: expand this message!)')")
                varargcode.unindent(2)
                varargcode.write("elements = []")
                varargcode.write("for arg in args:", post_indent=True)
                varargcode.write("if isinstance(arg, list):", post_indent=True)
                varargcode.write("for a in arg:", post_indent=True)
                varargcode.write("elements.append(a" + accessor + ")")
                varargcode.unindent(2)
                varargcode.write("else:", post_indent=True)
                varargcode.write("elements.append(arg" + accessor + ")")
                varargcode.unindent(2)
                varargcode.write("array = self.gateway.new_array(self.gateway.jvm." + type_name + ", len(elements))")
                varargcode.write("cnt = 0")
                varargcode.write("for element in elements:", post_indent=True)
                varargcode.write("array[cnt] = element")
                varargcode.write("cnt += 1")
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

                call_name, it, et, accessor = _method_name(type_name, var_name, default_value)
                iftest += it

            call += [call_name]

        if len(varargcode) > 0:
            generator.append(varargcode)

        if len(iftest) > 0:
            generator.write(iftest + ":")
        generator.indent()

        if is_reverse:
            generator.write("op = cls.rcreate(" + ", ".join(call) + ')', post_unindent=True)
        else:
            generator.write("op = cls.create(" + ", ".join(call) + ')', post_unindent=True)

    generator.write("else:", post_indent=True)
    generator.write("raise Exception('input types differ (TODO: expand this message!)')", post_unindent=True)
    # code += "    import inspect\n"
    # code += "    method = inspect.stack()[0][3]\n"
    # code += "    print(method)\n"


def _method_name(type_name, var_name, default_value):
    if type_name == "String":
        phrase = " type(" + var_name + ") is str"
        if (default_value is not None and default_value == 'None'):
            iftest = " (" + var_name + " is None or" + phrase + ")"
        else:
            iftest = phrase
        # call_name = "str(" + var_name + ")"
        call_name = "str(" + var_name + ") if (" + var_name + " is not None) else None"
        excepttest = "not" + iftest
        accessor = ""
    elif type_name == "double" or type_name == "float":
        iftest = " isinstance(" + var_name + ", (int, long, float))"
        # call_name = "float(" + var_name + ")"
        call_name = "float(" + var_name + ") if (" + var_name + " is not None) else None"
        excepttest = "not" + iftest
        accessor = ""
    elif type_name == "long":
        iftest = " isinstance(" + var_name + ", (int, long, float))"
        # call_name = "long(" + var_name + ")"
        call_name = "long(" + var_name + ") if (" + var_name + " is not None) else None"
        excepttest = "not" + iftest
        accessor = ""
    elif type_name == "int" or type_name == "Short" or type_name == "Char":
        iftest = " isinstance(" + var_name + ", (int, long, float))"
        # call_name = "int(" + var_name + ")"
        call_name = "int(" + var_name + ") if (" + var_name + " is not None) else None"
        excepttest = "not" + iftest
        accessor = ""
    elif type_name == "boolean":
        iftest = " isinstance(" + var_name + ", (int, long, float, str))"
        call_name = "True if " + var_name + " else False"
        excepttest = "not" + iftest
        accessor = ""
    elif type_name.endswith("MapOp"):
        base_var = var_name
        var_name += ".mapop"
        phrase = " hasattr(" + base_var + ", 'mapop') and self.is_instance_of(" + var_name + ", '" + type_name + "')"
        if (default_value is not None and default_value == 'None'):
            iftest = " (" + var_name + " is None or" + phrase + ")"
        else:
            iftest = phrase
        call_name = var_name
        excepttest = " hasattr(" + base_var + ", 'mapop') and not self.is_instance_of(" + \
                     var_name + ", '" + type_name + "')"
        accessor = ".mapop"
    else:
        phrase = " self.is_instance_of(" + var_name + ", '" + type_name + "')"
        if (default_value is not None and default_value == 'None'):
            iftest = " (" + var_name + " is None or" + phrase + ")"
        else:
            iftest = phrase
        call_name = var_name
        excepttest = "not" + iftest
        accessor = ""

    return call_name, iftest, excepttest, accessor


def _generate_methods(instance, signatures):
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


def _in_signature(param, signature):
    for s in signature:
        if s[0] == param[0]:
            if s[1] == param[1]:
                if s[3] == param[3]:
                    return True
                else:
                    raise Exception("only default values differ: " + str(s) + ": " + str(param))
            else:
                raise Exception("type parameters differ: " + s[1] + ": " + param[1])
    return False


def _generate_oo_signature(methods):
    signature = []
    dual = len(methods) > 1
    for method in methods:
        for param in method:
            # print("Param: " + str(param))
            if not param[2] == "self" and not _in_signature(param, signature):
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
                if s[3].endswith("NaN"):
                    sig += [s[0] + "=float('nan')"]
                else:
                    sig += [s[0] + "=" + s[3]]
            elif dual:
                sig += [s[0] + "=None"]
            else:
                sig += [s[0]]

    return ",".join(sig)


def _generate_proc_signature(methods):
    signature = []
    dual = len(methods) > 1
    self_var = None
    for method in methods:
        for param in method:
            # print("Param: " + str(param))
            if (not param[2] == "self" or self_var == None) and not _in_signature(param, signature):
                signature.append(param)
                if param[2] == "self" and self_var == None:
                    self_var = param[0]
                if param[4]:
                    # var args must be the last parameter
                    break

    sig = []
    for s in signature:
        if s[4]:
            sig += ["*args"]
        else:
            if s[3] is not None:
                if s[3].endswith("NaN"):
                    sig += [s[0] + "=float('nan')"]
                else:
                    sig += [s[0] + "=" + s[3]]
            elif dual and s[0] != self_var:
                sig += [s[0] + "=None"]
            else:
                sig += [s[0]]

    return ",".join(sig), self_var
