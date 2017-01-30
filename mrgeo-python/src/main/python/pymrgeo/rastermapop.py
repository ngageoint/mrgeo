import copy

from pymrgeo.instance import is_instance_of as iio


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

    @staticmethod
    def nan():
        return float('nan')

    def clone(self):
        return copy.copy(self)

    def is_instance_of(self, java_object, java_class):
        return iio(self.gateway, java_object, java_class)
