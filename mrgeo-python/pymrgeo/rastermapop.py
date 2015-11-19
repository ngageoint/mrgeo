
import py4j


class RasterMapOp(object):

    mapop = None
    gateway = None
    context = None
    job = None


    def __init__(self, gateway=None, context=None, mapop = None, job = None):
        self.gateway = gateway
        self.context = context
        self.mapop = mapop
        self.job= job

