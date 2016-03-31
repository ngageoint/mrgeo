
from py4j.java_gateway import JavaClass, JavaObject

import copy
from py4j.java_gateway import JavaClass
import time

class RasterMapOp(object):

    mapop = None
    gateway = None
    context = None
    job = None

    def __init__(self, gateway=None, context=None, mapop=None, job=None):
        self.gateway = gateway
        self.context = context
        self.mapop = mapop
        self.job = job

    def clone(self):
        return copy.copy(self)

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
            return False
            #raise Exception("java_object must be a JavaClass, or a JavaObject")

        if cls.getCanonicalName() == name:
            return True

        return self._is_instance_of(cls.getSuperclass(), name)

    def _is_instance_of(self, clazz, name):
        if clazz:
            if clazz.getCanonicalName() == name:
                return True

            return self._is_instance_of(clazz.getSuperclass(), name)

        return False

    def eexport(self,name,singleFile=False,zoom=-1,numTiles=-1,mosaic=-1,format="tif",randomTiles=False,tms=False,colorscale="",tileids="",bounds="",allLevels=False,overridenodata=float('-inf')):
        import copy
        from numbers import Number
        import base64
        import numpy
        from osgeo import gdal_array
        import zlib
        from osgeo import gdal
        from py4j.java_gateway import JavaClass
        cls = JavaClass('org.mrgeo.mapalgebra.ExportMapOp', gateway_client=self.gateway._gateway_client)
        local_name = name
        name = 'In-Memory'
        if hasattr(self, 'mapop') and self.is_instance_of(self.mapop, 'org.mrgeo.mapalgebra.raster.RasterMapOp') and type(name) is str and isinstance(singleFile, (int, long, float, str)) and isinstance(zoom, (int, long, float)) and isinstance(numTiles, (int, long, float)) and isinstance(mosaic, (int, long, float)) and type(format) is str and isinstance(randomTiles, (int, long, float, str)) and isinstance(tms, (int, long, float, str)) and type(colorscale) is str and type(tileids) is str and type(bounds) is str and isinstance(allLevels, (int, long, float, str)) and isinstance(overridenodata, (int, long, float)):
            op = cls.create(self.mapop, str(name), True if singleFile else False, int(zoom), int(numTiles), int(mosaic), str(format), True if randomTiles else False, True if tms else False, str(colorscale), str(tileids), str(bounds), True if allLevels else False, float(overridenodata))
        else:
            raise Exception('input types differ (TODO: expand this message!)')
        if (op.setup(self.job, self.context.getConf()) and
                op.execute(self.context) and
                op.teardown(self.job, self.context.getConf())):
            new_resource = copy.copy(self)
            new_resource.mapop = op
            gdalutils = JavaClass('org.mrgeo.utils.GDALUtils', gateway_client=self.gateway._gateway_client)
            java_image = op.image()
            width = java_image.getRasterXSize()
            height = java_image.getRasterYSize()
            options = []
            if format == 'jpg' or format == 'jpeg':
                driver_name = 'jpeg'
                extension = 'jpg'
            elif format == 'tif' or format == 'tiff' or format == 'geotif' or format == 'geotiff' or format == 'gtif'  or format == 'gtiff':
                driver_name = 'GTiff'
                options.append('INTERLEAVE=BAND')
                options.append('COMPRESS=DEFLATE')
                options.append('PREDICTOR=1')
                options.append('ZLEVEL=6')
                options.append('TILES=YES')
                if width < 2048:
                    options.append('BLOCKXSIZE=' + str(width))
                else:
                    options.append('BLOCKXSIZE=2048')
                if height < 2048:
                    options.append('BLOCKYSIZE=' + str(height))
                else:
                    options.append('BLOCKYSIZE=2048')

                extension = 'tif'

            else:
                driver_name = format
                extension = format

            datatype = java_image.GetRasterBand(1).getDataType()

            if not local_name.endswith(extension):
                local_name += "." + extension

            driver = gdal.GetDriverByName(driver_name)
            local_image = driver.Create(local_name, width, height, java_image.getRasterCount(), datatype, options)
            local_image.SetProjection(str(java_image.GetProjection()))
            local_image.SetGeoTransform(java_image.GetGeoTransform())

            java_nodatas = gdalutils.getnodatas(java_image)

            for i in xrange(1, local_image.RasterCount + 1):
                start = time.time()
                raw_data = gdalutils.getRasterDataAsCompressedBase64(java_image, i, 0, 0, width, height)
                print('compressed/encoded data ' + str(len(raw_data)))

                decoded_data = base64.b64decode(raw_data)
                print('decoded data ' + str(len(decoded_data)))

                decompressed_data = zlib.decompress(decoded_data, 16 + zlib.MAX_WBITS)
                print('decompressed data ' + str(len(decompressed_data)))

                byte_data = numpy.frombuffer(decompressed_data, dtype='b')
                print('byte data ' + str(len(byte_data)))

                image_data = byte_data.view(gdal_array.GDALTypeCodeToNumericTypeCode(datatype))
                print('gdal-type data ' + str(len(image_data)))

                image_data = image_data.reshape((-1, width))
                #print('reshaped ' + str(len(byte_data)))
                #print(byte_data)

                #for j in xrange(0, 10):
                #    print(byte_data[j])
                print('reshaped ' + str(len(image_data)) + " x " + str(len(image_data[0])))
                band = local_image.GetRasterBand(i)

                print('writing local image')
                band.WriteArray(image_data)
                print('done')
                end = time.time()

                print("elapsed time: " + str(end - start) + " sec.")

                band.SetNoDataValue(java_nodatas[i - 1])

            local_image.FlushCache()
            print('flushed cache')

            return new_resource
        return None
