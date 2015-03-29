from thunder.streaming.shell.analysis import Analysis

from abc import abstractmethod
from collections import OrderedDict
import numpy as np
import re
import os
import json
import struct
import time

# TODO Fix up comment
"""
A converter takes the raw output from an Analysis (a collection of ordered binary files) and combines them into a
usable format (i.e. an image, or a JSON document to send to Lightning).

Every converter must be decorated with the @converter decorator, so that they will be dynamically added to the
Analysis class. Doing this enables the following workflow:

*********************************************************

What's the most desirable output pattern?

analysis = Analysis.ExampleAnalysis(...).toSeries()
                .toImageFile(path)
                .toLightningServer(lgn)

or...

analysis = Analysis.ExampleAnalysis(...).toSeries()
output1 = LightningOutput(lgn)
output2 = ImagesOutput(path)
analysis.addOutputs(output1, output2)

*********************************************************

(output_loc is an interface for receiving the output data from an Analysis. It should be pluggable (so that we can
read the parts from disk, or from the network when that's available)

@converter
def seriesToImage(analysis):
    (parse the files in the Analysis' output directory and convert them into an image)

analysis1 = Analysis.ExampleAnalysis(param1=value1, param2=value2...).seriesToImage()

"""


class Data(object):
    """
    An abstract base class for all objects returned by a converter function. Output objects can define a set of
    functions to send their contents to external locations:

    # converted defines the type-specific output function
    converted = Analysis.ExampleAnalysis(...).toSeries()

    Retains a reference to the Analysis that created it with self.analysis (this is used to start the Analysis
    thread)
    """

    @property
    def identifier(self):
        return self.__class__.__name__

    def __init__(self, analysis):
        self.analysis = analysis
        # Output functions are added to output_funcs with the @output decorator
        self.output_funcs = {}

    @staticmethod
    def output(func):
        def add_to_output(self, *args, **kwargs):
            self.output_funcs[func.func_name] = lambda data: func(self, data, *args, **kwargs)
            return self.analysis
        return add_to_output

    @staticmethod
    def converter(func):
        """
        :param func: A function with a single parameter type, Analysis, that must return an instance of Data
        :return: The function after it's been added to Analysis' dict
        """
        def add_output(analysis, **kwargs):
            output = func(analysis, **kwargs)
            analysis.outputs.append(output)
            return output
        print "Adding %s to Analysis.__dict__" % func.func_name
        setattr(Analysis, func.func_name, add_output)
        return func

    @abstractmethod
    def _convert(self, root, new_data):
        return None

    def handle_new_data(self, root, new_data):
        converted = self._convert(root, new_data)
        for func in self.output_funcs.values():
            func(converted)

    def start(self):
        self.analysis.start()

    def stop(self):
        self.analysis.stop()


# Some example Converters for StreamingSeries

class Series(Data):

    DIMS_FILE_NAME = "dimensions.json"
    RECORD_SIZE = "record_size"
    DTYPE = "dtype"
    DIMS_PATTERN = re.compile(DIMS_FILE_NAME)

    def __init__(self, analysis, dtype="float16"):
        Data.__init__(self, analysis)
        self.dtype = dtype

    @staticmethod
    @Data.converter
    def toSeries(analysis):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory series
        :return: A Series object
        """
        return Series(analysis)

    def _get_dims(self, root):
        try:
            dims = open(os.path.join(root, Series.DIMS_FILE_NAME), 'r')
            dims_json = json.load(dims)
            record_size = int(dims_json[self.RECORD_SIZE])
            dtype = dims_json[self.DTYPE]
            return record_size, dtype
        except Exception as e:
            print "Cannot load binary series: %s" % str(e)
            return None, None

    def _loadBinaryFromPath(self, p, record_size, dtype):
        # Load each line according to record_size and dtype
        fbuf = open(p, 'rb').read()
        fsize = len(fbuf)
        ptr = 0
        while fsize - ptr != 0:
            idx = struct.unpack("<i", fbuf[ptr:(ptr + 4)])[0]
            buf = np.frombuffer(fbuf, dtype=dtype, count=record_size, offset=ptr + 4)
            ptr += 4 + record_size * 8
            yield idx, buf

    def _saveBinaryToPath(self, p, data):
        with open(p, 'wb') as f:
            vals = None
            if isinstance(data, dict):
                vals = map(lambda x: data[x], sorted(data))
            elif isinstance(data, list):
                vals = sorted(data)
            arr = np.array(vals)
            np.save(f, arr)

    def _convert(self, root, new_data):
        records = OrderedDict()

        # Load in the dimension JSON file (which we assume exists in the results directory)
        record_size, dtype = self._get_dims(root)
        if not record_size or not dtype:
            return None
        self.dtype = dtype

        for f in new_data:
            # Make sure to exclude the dimensions file
            if not self.DIMS_PATTERN.search(f):
                # Load each line according to record_size and dtype
                for idx, buf in self._loadBinaryFromPath(f, record_size, dtype):
                    records[idx] = buf
        return records

    @Data.output
    def toLightning(self, data, lgn, only_viz=False):
        if data is None or len(data) == 0:
            return
        # Make sure the values are sorted
        sorted_vals = map(lambda x: data[x], sorted(data))
        arr_values = np.array(sorted_vals)
        if only_viz:
            print "Appending %s to existing line viz." % str(arr_values)
            lgn.append(arr_values)
        else:
            # Do dashboard stuff here
            lgn.line(arr_values)

    @Data.output
    def toFile(self, path, data, prefix=None, fileType='bin'):
        """
        If prefix is specified, a different file will be written out at every batch. If not, the same image file will be
        overwritten.
        """
        # TODO implement saving with keys as well
        fullPath = path if not prefix else path + '-' + time.time()
        fullPath = fullPath + '.' + fileType
        self._saveBinaryToPath(fullPath, data)


class Image(Series):
    """
    Represents a 2 or 3 dimensional image
    """

    def __init__(self, analysis, dims, clip):
        Series.__init__(self, analysis)
        self.dims = dims
        self.clip = clip

    @staticmethod
    @Data.converter
    def toImage(analysis, dims=(512, 512, 4), clip=400):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory image
        :return: An Image object
        """
        return Image(analysis, dims, clip)

    def _convert(self, root, new_data):
        records = Series._convert(self, root, new_data)
        if records is not None:
            # Sort the keys/values
            sorted_vals = map(lambda x: records[x], sorted(records))
            only_vals = [value[0] for value in sorted_vals]
            image_arr = np.asarray(only_vals).clip(0, self.clip).reshape(self.dims)
            return image_arr

    def _getPlaneData(self, data, plane):
        plane_size = self.dims[0] * self.dims[1]
        return data[plane * plane_size:(plane + 1) * plane_size]

    @Data.output
    def toLightning(self, data, lgn, plane=0, only_viz=False):
        if data is None or len(data) == 0:
            return
        if len(self.dims) > 3 or len(self.dims) < 1:
            print "Invalid images dimensions (must be < 3 and >= 1)"
            return
        plane_data = self._getPlaneData(data, plane)
        if only_viz:
            lgn.update(plane_data)
        else:
            # Do dashboard stuff here
            lgn.image(plane_data)

    @Data.output
    def toFile(self, path, data, prefix=None, fileType='img'):
        Series._saveBinaryToPath(self, prefix, fileType)

