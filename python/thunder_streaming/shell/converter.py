from thunder_streaming.shell.analysis import Analysis

from abc import abstractmethod
from collections import OrderedDict
from thunder import Colorize
import numpy as np
from numpy import cumprod, max
from scipy.signal import decimate
from math import ceil
import re
import os
import json
import struct
import time
from tifffile import imsave

from multiprocessing import Pool

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

class DataProxy(object):
    """
    Instead of an Analysis maintaining a fixed reference to a particular Data object, it should keep a reference to
    a DataProxy, which can then be updated to point to different Data instances if necessary. This pattern enables
    type conversion among Data instances after they've been added to an Analysis' 'outputs' list.

    The motivating use-case for this class is of an Analysis which returns two outputs, a Series and an Image, but the
    Analysis.getMultiValue converter assumes the return types are both Series. Converting the Series output to an Image
    after getMultiValue is called will require Analysis' reference to be updated.
    """

    def __init__(self, data_obj):
        self.data_obj = data_obj
        self.data_obj.set_proxy(self)

    def update_reference(self, data_obj):
        self.data_obj = data_obj

    def handle_new_data(self, new_data):
        self.data_obj.handle_new_data(new_data)


class Data(object):
    """
    An abstract base class for all objects returned by a converter function. Output objects can define a set of
    functions to send their contents to external locations:

    # converted defines the type-specific output function
    converted = Analysis.ExampleAnalysis(...).toSeries()

    Retains a reference to the Analysis that created it with self.analysis (this is used to start the Analysis
    thread)
    """

    @staticmethod
    def output(func):
        def add_to_output(self, *args, **kwargs):
            self.output_funcs[func.func_name] = lambda data: func(self, data, *args, **kwargs)
            return self.analysis
        return add_to_output

    @staticmethod
    def transformation(func):
        def add_to_transformations(self, *args, **kwargs):
            self.transformation_funcs.append(lambda data: func(self, data, *args, **kwargs))
            return self
        return add_to_transformations

    @staticmethod
    def converter(func):
        """
        :param func: A function with a single parameter type, Analysis, that must return an instance of DataProxy
        :return: The function after it's been added to Analysis' dict
        """
        def add_output(analysis, **kwargs):
            output_proxies = func(analysis, **kwargs)
            if isinstance(output_proxies, list):
                analysis.outputs.extend(output_proxies)
                return [proxy.data_obj for proxy in output_proxies]
            else:
                analysis.outputs.append(output_proxies)
                return output_proxies.data_obj
        print "Adding %s to Analysis.__dict__" % func.func_name
        setattr(Analysis, func.func_name, add_output)
        return func

    def __init__(self, analysis):
        self.analysis = analysis
        # Keep a reference to the proxy that self.analysis maintains (set after initialization)
        self.proxy = None
        # Output functions are added to output_funcs with the @output decorator
        self.output_funcs = {}
        # Transformation functions are applied to the input data before its passed to any output_funcs
        self.transformation_funcs = []

    @property
    def identifier(self):
        return self.__class__.__name__

    @staticmethod
    @abstractmethod
    def convert(new_data):
        """ 
        Converts new_data (a dict of raw bytes) into a numpy array of the correct format according to the data type

        @staticmethod so that conversions can be consolidated if there are multiple converters of the same type 
        for a given Analysis
        """
        return None

    @abstractmethod
    def postprocess(self, new_data): 
        """ 
        Do any instance-specific post-conversion steps here (necessary because convert is a static method)
        """ 
        pass

    def _propagate_values(self, other, **kwargs):
        for key, value in vars(self).items():
            # Keyword arguments take precedent over attribute propagation
            if key not in kwargs:
                setattr(other, key, value)

    def set_proxy(self, proxy):
        self.proxy = proxy

    def handle_new_data(self, new_data):
        # new_data has already been converted inside Analysis.handle_new_data
        transformed = self.postprocess(converted)
        for func in self.transformation_funcs:
            transformed = func(transformed)
        for func in self.output_funcs.values():
            func(transformed)

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

    def __init__(self, analysis, dtype="float16", index=None):
        Data.__init__(self, analysis)
        self.dtype = dtype
        self.index = index

    @staticmethod
    @Data.converter
    def toSeries(analysis, index=None):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory series
        :return: A DataProxy pointing to a Series object
        """
        series = Series(analysis, index=index)
        return DataProxy(series)

    def toImage(self, **kwargs):
        """
        Since Analysis objects only maintain references to proxies for Data objects, the toImage conversion need only
        update that proxy's reference

        Once a Series is converted into an Image, the original Series will no longer receive new data from the Analysis
        """
        image = Image(self.analysis, **kwargs)
        # Transfer all of self's attributes over the to the new Image
        self._propagate_values(image, **kwargs)
        self.proxy.update_reference(image)
        return image

    @staticmethod
    def _get_dims(new_data):
        try:
            dims = new_data.get(Series.DIMS_FILE_NAME)
            dims_json = json.load(dims)
            record_size = int(dims_json[self.RECORD_SIZE])
            dtype = dims_json[self.DTYPE]
            return record_size, dtype
        except Exception as e:
            print "Cannot load binary series: %s" % str(e)
            return None, None

    def _saveBinaryToPath(self, p, data):
        print "In _saveBinaryToPath, saving to: %s" % p
        if data is None or len(data) == 0:
            return
        with open(p, 'w+') as f:
            f.write(data)

    @staticmethod
    def convert(new_data):

        num_output_files = len(new_data) - 1

        def get_partition_num(output_name):
            split_name = output_name.split('-')
            if len(split_name) == 3:
                return int(split_name[1])
            return num_output_files

        # Load in the dimension JSON file (which we assume exists in the results directory)
        record_size, dtype = Series._get_dims(new_data)
        if not record_size or not dtype:
            return None
        self.dtype = dtype

        without_dims = filter(lambda x: not Series.DIMS_PATTERN.search(x), new_data)
        sorted_files = sorted(without_dims, key=get_partition_num)
        bytes = ''
        for f in sorted_files:
            series = new_data[f]
            bytes = bytes + series
        merged_series = np.frombuffer(bytes, dtype=dtype)
        reshaped_series = merged_series.reshape(-1, record_size)
    
    def postprocess(self, converted): 
        reshaped_series = converted 
        if self.index:
            # Slice out the relevant values from the reshaped series
            reshaped_series = reshaped_series[:, self.index]

        if len(reshaped_series) > 2: 
            print "In Series.convert, first 2 elements: %s" % str(reshaped_series[:2])

        return reshaped_series

    @Data.output
    def toLightning(self, data, lgn, only_viz=False):
        if data is None or len(data) == 0:
            return
        if only_viz:
            print "Appending %s to existing line viz." % str(data)
            lgn.append(data)
        else:
            # Do dashboard stuff here
            lgn.line(data)

    @Data.output
    def toFile(self, data, path=None, prefix=None, fileType='bin'):
        """
        If prefix is specified, a different file will be written out at every batch. If not, the same image file will be
        overwritten.
        """
        # TODO implement saving with keys as well
        if data is None: 
            return
        if path and len(data) > 0:
            fullPath = path if not prefix else path + '-' + str(time.time())
            fullPath = fullPath + '.' + fileType
            if fileType == 'bin':
                self._saveBinaryToPath(fullPath, data)
            elif fileType == 'tif':
                imsave(fullPath, data, bigtiff=True)
        else:
            print "Path or data not specified in toFile."

    @Data.transformation
    def apply(self, data, func=None): 
        return func(data)

    @Data.transformation
    def getRegressors(self, data, num_regressors=None): 
        if data is None or len(data) == 0 or not num_regressors:
            return
        print "In getRegressors, data.shape: %s" % str(data.shape)
        regressors = data[-num_regressors:]
        return regressors
        

class MultiValue(Series):
    """
    Certain analyses will return multiple outputs (two different series objects, for example) which must be unpacked. The
    MultiValue class provides the ability to "fork" Analysis outputs into multiple output pipelines
    """

    @Data.converter
    def getMultiValues(analysis, sizes=[]):
        from numpy import cumsum
        slices = [slice(x[0], x[1], 1) for x in zip([0] + list(cumsum(sizes)), cumsum(sizes))]
        return [DataProxy(Series(analysis, index=s)) for s in slices]


class Image(Series):
    """
    Represents a 2 or 3 dimensional image
    """
    def __init__(self, analysis, dims=None, preslice=None):
        Series.__init__(self, analysis)
        self.dims = dims
        self.preslice = preslice

    @staticmethod
    @Data.converter
    def toImage(analysis, dims=(512, 512, 4), preslice=None):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory image
        :return: An Image object
        """
        image = Image(analysis, dims=dims, preslice=preslice)
        return DataProxy(image)

    @staticmethod
    def convert(new_data):
        return Series.convert(new_data)

    def postprocess(self, series): 
        if series is not None and len(series) != 0:
            # Remove the regressors
            if self.preslice:
                series = series[self.preslice]
            # Sort the keys/values
            print "series.shape: %s" % str(series.shape)
            image_arr = series.reshape(self.dims, order='C')
            print "_convert returning array of shape %s" % str(image_arr.shape)
            return image_arr

    @Data.transformation
    def downsample(self, data, factor=4):
        curData = data
        for idx, dim in enumerate(data.shape):
            curData = decimate(curData, int(max(1, ceil(factor ** (1.0 / data.ndim)))), axis=idx)
        return curData

    @Data.transformation
    def colorize(self, data, cmap="hsv", scale=1, vmin=0, vmax=30, mask_with_first=False, threshold=0.05):
        if data is None or len(data) == 0:
            return None
        print "In colorize, data.shape: %s" % str(data.shape)
        mask_img = None
        # TODO: This should ideally accept any index as a mask 
        if mask_with_first: 
            mask_img = data[0, ...]
            mask_img = mask_img > threshold
            data = data[1:, ...]
        return Colorize(cmap=cmap, scale=scale, vmin=vmin, vmax=vmax).transform(data, mask=mask_img)

    @Data.transformation
    def getPlane(self, data, plane):
        if data is None or len(data) == 0:
            return None
        if data.ndim == 3: 
            return data[plane, :, :]
        else: 
            # Assume the first dimension is the value per pixel 
            return data[:, plane, ...]

    @Data.transformation
    def clip(self, data, min, max):
        if data is None or len(data) == 0:
            return None
        return data.clip(min, max)

    @Data.transformation
    def normalize(self, data): 
        """ 
        How to do this without computing expensive statistics in the driver? 
        Need to use the results of a streaming analysis? 
        """ 
        pass

    @Data.transformation
    def maxProject(self, data, axis=0, keepdims=True):
        if data is None or len(data) == 0: 
            return None
        return max(data, axis=axis, keepdims=keepdims)

    @Data.output
    def toLightning(self, data, image_viz, image_dims, only_viz=False):
        if data is None or len(data) == 0:
            return
        print "Sending data with dims: %s to Lightning" % str(data.shape)
        if only_viz:
            image_viz.update(data)
        else:
            # Do dashboard stuff here
            lgn.image(data)
