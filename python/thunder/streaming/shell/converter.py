from thunder.streaming.shell.analysis import Analysis
from numpy import array

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

def converter(func):
    """
    :param func: A function with a single parameter type, Analysis, that must return an Analysis
    :return: The function after it's been added to Analysis' dict
    """
    Analysis.__dict__[func.func_name] = func
    return func


class Data(object):
    """
    An abstract base class for all objects returned by a converter function. Output objects can define a set of
    functions to send their contents to external locations:

    # converted defines the type-specific output function
    converted = Analysis.ExampleAnalysis(...).toSeries()

    """
    pass


# Some example Converters for StreamingSeries

class Series(Data):

    def __init__(self, nparray):
        self.array = nparray

    @converter
    @staticmethod
    def toSeries(analysis):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory series
        :return: A Series object
        """
        output_dir = analysis.output_loc


    def toLightningServer(self):
        pass

    def toFile(self, path):
        pass


class Image(Data):

    @converter
    @staticmethod
    def toImage(analysis):
        """
        :param analysis: The analysis whose raw output will be parsed and converted into an in-memory image
        :return: An Image object
        """

    def toLightningServer(self):
        pass

    def toFile(self, path):
        pass








