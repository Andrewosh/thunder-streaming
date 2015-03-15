from thunder.streaming.shell.analysis import Analysis
from abc import abstractmethod
import numpy as np
import re
import os
import json
from collections import OrderedDict

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
        def add_to_output(self, *args):
            self.output_funcs[func.func_name] = lambda data: func(self, data, *args)
            return self.analysis
        return add_to_output

    @staticmethod
    def converter(func):
        """
        :param func: A function with a single parameter type, Analysis, that must return an instance of Data
        :return: The function after it's been added to Analysis' dict
        """
        def add_output(analysis):
            output = func(analysis)
            analysis.outputs.append(output)
            return output
        print "Adding %s to Analysis.__dict__" % func.func_name
        setattr(Analysis, func.func_name, add_output)
        return func

    @abstractmethod
    def _convert(self, root, new_data):
        pass

    def handle_new_data(self, new_data):
        converted = self._convert(new_data)
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

    @staticmethod
    @Data.converter
    def toSeries(analysis):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory series
        :return: A Series object
        """
        return Series(analysis)

    def _convert(self, root, new_data):
        # Load in the dimension JSON file (which we assume exists in the results directory)
        record_size, dtype = None, None
        records = OrderedDict()

        try:
            dims = open(os.path.join(root, Series.DIMS_FILE_NAME), 'r')
            dims_json = json.load(dims)
            record_size = dims_json[self.RECORD_SIZE]
            dtype = dims_json[self.DTYPE]
        except Exception as e:
            print "Cannot load binary series: %s" % str(e)
        if not record_size or not dtype:
            return

        for f in new_data:
            # Make sure to exclude the dimensions file
            if not f.search(self.DIMS_FILE_NAME):
                # Load each line according to record_size and dtype
                fbuf = open(f, 'rb').read()
                fsize = len(fbuf)
                ptr = 0
                while fsize - ptr != 0:
                    idx = int(fbuf[ptr:ptr + 4])
                    buf = np.frombuffer(fbuf, dtype=dtype, count=record_size, offset=ptr + 4)
                    records[idx] = buf

        return records.keys(), records.values()

    @Data.output
    def toLightning(self, lgn, data):
        # TODO: just debugging for now
        print "In toLightningServer, data: %s" % str(data)

    @Data.output
    def toFile(self, path, data):
        return self


class Image(Data):

    @staticmethod
    @Data.converter
    def toImage(analysis):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory image
        :return: An Image object
        """
        return Image(analysis)

    def _convert(self, root, new_data):
        pass

    @Data.output
    def toLightning(self, lgn, data):
        pass

    @Data.output
    def toFile(self, path, data):
        pass








