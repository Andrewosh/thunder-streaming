from thunder.streaming.shell.mapped_scala_class import MappedScalaClass
from thunder.streaming.shell.param_listener import ParamListener
import settings
from threading import Thread
import time


class Analysis(MappedScalaClass, ParamListener, Thread):
    """
    This class is dynamically modified by ThunderStreamingContext when it's initialized with a JAR file

    An Analysis must have a corresponding thread that monitors its output directory and sends "new data" notifications
    to any converters which have been registered on it.
    """

    SUBSCRIPTION_PARAM = "dr_subscription"
    FORWARDER_ADDR_PARAM = "dr_forwarder_addr"

    # Necessary Analysis parameters
    OUTPUT = "output"

    # Period with which the Analysis will check for new data
    POLL_PERIOD = 1

    def __init__(self, identifier, full_name, param_dict):
        super(Analysis, self).__init__(identifier, full_name, param_dict)
        if Analysis.OUTPUT not in self._param_dict:
            print "An Analysis has been created without an output location. The results of this Analysis will\
             not be usable"
        # self.output is the location which is monitored for new data
        self.output = self._param_dict[Analysis.OUTPUT]
        # The output dictionary is updated in methods that are dynamically inserted into Analysis.__dict__ using the
        # @converter decorator
        self.outputs = {}

        # Fields involved in directory monitoring
        self._last_dir_state = None
        self._stopped = False

        # Put the address of the subscription forwarder into the parameters dict
        self._param_dict[Analysis.FORWARDER_ADDR_PARAM] = "tcp://" + settings.MASTER  + ":" + str(settings.PUB_PORT)
        self.receive_updates(self)

    def add_converter(self, converter):
        self.converters[converter.identifier] = converter

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

    def run(self):
        while not self._stopped:
            cur_dir_state = set(os.listdir(self.output))
            if cur_dir_state != self._last_dir_state:
                diff = cur_dir_state.difference(self._last_dir_state)
                for output in self.outputs.values():
                    output.handle_new_data(diff)
                    # TODO finish new data handling
                self._last_dir_state = cur_dir_state
            time.sleep(Analysis.POLL_PERIOD)

    def stop(self):
        self._stopped = True

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