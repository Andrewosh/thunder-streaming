from thunder_streaming.shell.mapped_scala_class import MappedScalaClass
from thunder_streaming.shell.param_listener import ParamListener
from thunder_streaming.shell.file_monitor import FileMonitor
import settings
from multiprocessing import Pool
import time
import os

class Analysis(MappedScalaClass, ParamListener):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file

    An Analysis must have a corresponding thread that monitors its output directory and sends "new data" notifications
    to any converters which have been registered on it.
    """

    SUBSCRIPTION_PARAM = "dr_subscription"
    FORWARDER_ADDR_PARAM = "dr_forwarder_addr"

    # Necessary Analysis parameters
    OUTPUT = "output"

    NUM_CHILD_PROCS = 5
    POOL = Pool(NUM_CHILD_PROCS)

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        if Analysis.OUTPUT not in self._param_dict:
            print "An Analysis has been created without an output location. The results of this Analysis will\
             not be usable"
        # self.output is the location which is monitored for new data
        self.output_dir = self._param_dict[Analysis.OUTPUT]
        # The output dictionary is updated in methods that are dynamically inserted into Analysis.__dict__ using the
        # @converter decorator
        self.outputs = []

        self.file_monitor = FileMonitor.getInstance(self)

        # Put the address of the subscription forwarder into the parameters dict
        self._param_dict[Analysis.FORWARDER_ADDR_PARAM] = "tcp://" + settings.MASTER  + ":" + str(settings.PUB_PORT)
        self.receive_updates(self)

    def get_output_dir(self):
        return self.output_dir

    def get_outputs(self):
        return self.outputs

    def add_output(self, output):
        self.outputs[output.identifier] = output

    def handle_new_data(self, data): 
        """
        Called by this Analysis' FileMonitor, handle_new_data takes a loaded batch of data (in dict form), 
        converts it according to the Converter.convert functions of its Converters, and then passes it to 
        the Analysis' outputs.
        """

        # Do all base conversions (conversions that can be shared across all instances of a particular data type, i.e.
        # the Series conversion)
        data_types = set()
        for proxy in self.outputs: 
            data_types.add(proxy.data_obj.__class__)
        converted = {}
        for data_type in data_types: 
            converted[data_type.__name__] = data_type.convert(data)

        # Pass the initial conversion results into instance-specific postprocessing
        def postprocess_then_output(converter, data): 
            converter_type = converter.data_obj.__class__
            converter.handle_new_data(converted[converter_type.__name__])
            
        Analysis.POOL.map(postprocess_then_output, self.outputs)

    def receive_updates(self, analysis):
        """
        Write a parameter to the Analysis' XML file that tells it to subscribe to updates from the given
        analysis.
        """
        existing_subs = self._param_dict.get(Analysis.SUBSCRIPTION_PARAM)
        identifier = analysis if isinstance(analysis, str) else analysis.identifier
        if not existing_subs:
            new_sub = [identifier]
            self.update_parameter(Analysis.SUBSCRIPTION_PARAM, new_sub)
        else:
            existing_subs.append(identifier)
            self.update_parameter(Analysis.SUBSCRIPTION_PARAM, existing_subs)

    def start(self):
        self.file_monitor.start()

    def stop(self):
        self.file_monitor.stop()

    def __repr__(self):
        desc_str = "Analysis: \n"
        desc_str += "  Identifier: %s\n" % self.identifier
        desc_str += "  Class: %s\n" % self.full_name
        desc_str += "  Parameters: \n"
        if self._param_dict:
            for (key, value) in self._param_dict.items():
                desc_str += "    %s: %s\n" % (key, value)
        return desc_str

    def __str__(self):
        return self.__repr__()
