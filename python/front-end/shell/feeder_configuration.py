import re
import os
from tempfile import NamedTemporaryFile
from itertools import chain
from collections import OrderedDict

THUNDER_STREAMING_PATH = os.environ.get("THUNDER_STREAMING_PATH")
FEEDER_DIR = "/python/thunderfeeder/"
GSS_FEEDER_PATH = os.path.join(THUNDER_STREAMING_PATH, FEEDER_DIR, "grouping_series_stream_feeder.py")
SS_FEEDER_PATH = os.path.join(THUNDER_STREAMING_PATH, FEEDER_DIR, "series_stream_feeder.py")

DEFAULT_TMP_DIR = "/groups/freeman/streamingtmp/"


class FeederConfiguration(object):
    """
    A FeederConfiguration contains all the information necessary to launch the feeder script, including whether to look
    for both imaging and behavioral data, the number of files to transfer per batch, and relevant file prefixes
    (specified by regexes).
    """

    class RegexList:
        """
        Wrapper for a set of regexes that will get written to a file
        """

        def __init__(self, regexes):
            self.regexes = regexes

        def __iter__(self):
            for regex in self.regexes:
                yield regex

    # Keyword parameters for the feeder script
    KW_PARAMS = {
        'mod_buffer_time': '--mod-buffer-time',
        'poll_time': '--poll-time',
        'linger_time': '--linger-time',
        'max_files': '--max-files',
        'image_prefix': '--imgprefix',
        'behaviors_prefix': '--behavprefix',
        'shape': '--shape',
        'linear': '--linear',
        'data_type': '--dtype',
        'index_type': '--indtype',
        'prefix_regexes': '--prefix-regex-file',
        'timepoint_regexes': '--timepoint-regex-file',
        'filter_regexes': '--filter-regex-file',
        'check_size': '--check-size',
        'no_check_skip': '--no-check-skip'
    }

    # Positional parameters are ordered and don't have '--' specifiers
    POS_PARAMS = OrderedDict({
        'images_dir': '',
        'behaviors_dir': '',
        'spark_input_dir': ''
    })

    # All entries in this dict are converted to environment variables immediately before the feeder script is started
    ENV_VAR_PARAMS = {
        'TMP_OUTPUT_DIR': ''
    }

    def __init__(self):
        self.params = {}

        # Add setters for each of the above parameter options to the configuration object dictionary
        def _update_param(n):
            def _set_update(v=''):
                self.params[n] = self._handle_value(n, v)
            return _set_update

        for (n, v) in chain(self.KW_PARAMS.items(), self.POS_PARAMS.items(), self.ENV_VAR_PARAMS.items()):
            self.__dict__['set_' + n.lower()] = _update_param(n)

        self.set_tmp_output_dir(DEFAULT_TMP_DIR)

    def _get_executable(self):
        if self.params.get('behaviors_dir'):
            return GSS_FEEDER_PATH
        else:
            return SS_FEEDER_PATH

    def _handle_value(self, name, value):
        """
        Once a parameter has been set, check to see if it requires any special handling (i.e. regexes need to be
        converted into regex files for compatibility with the feeder scripts)."
        :param value: A parameter from the above dictionary
        :return: The final string representation of that parameter that will be used in the CLI command
        """

        # Regexes need to be written to temporary files
        fre = re.compile(".*regexes")
        if fre.match(name):
            if isinstance(value, FeederConfiguration.RegexList):
                temp_file_name = None
                with NamedTemporaryFile(delete=False) as temp:
                    for regex in value:
                        temp.write(regex + '\n')
                    temp_file_name = temp.name
                return temp_file_name
            else:
                print "Can only write regexes in RegexList form"

        # The default is to convert the value to a str and pass it through
        return str(value)

    def generate_command(self):
        """
        :return: (vars, cmd) where 'vars' is a dict of environment variables to set and 'cmd' is the ready-to-execute
            launch command string for this feeder script configuration (just insert it into a Popen call).
        """

        def build_arg(param_dict, p):
            prefix = param_dict.get(p)
            val = self.params.get(p)
            if val:
                return prefix + " " + val
            return ''

        pos_args = [build_arg(self.POS_PARAMS, p) for p in self.POS_PARAMS.keys()]
        kw_args = filter(lambda x: x, [build_arg(self.KW_PARAMS, p) for p in self.KW_PARAMS.keys()])

        return (dict([(k, self.params.get(k)) for k in self.ENV_VAR_PARAMS.keys()]),
                list(chain([self._get_executable()], pos_args, kw_args)))

    def __str__(self):
        def params_to_str(param_dict, s):
            for key in param_dict.keys():
                val = self.params.get(key)
                if val:
                    s += "    %s: %s\n" % (key, val)
            return s
        s = "FeederConfiguration:\n"
        s += "  Environment Variables:\n"
        s = params_to_str(self.ENV_VAR_PARAMS, s)
        s += "  Positional Arguments:\n"
        s = params_to_str(self.POS_PARAMS, s)
        s += "  Keyword Arguments:\n"
        s = params_to_str(self.KW_PARAMS, s)
        return s

    def __repr__(self):
        return self.__str__()


"""
Example configurations
"""

# Nick's
NicksFeederConf = FeederConfiguration()
NicksFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/demo_2015_01_16/registered_im")
NicksFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/demo_2015_01_16/registered_bv")
NicksFeederConf.set_spark_input_dir("/nobackup/freeman/streaminginput/")
NicksFeederConf.set_max_files(40)
NicksFeederConf.set_linger_time(60.0)

# Nikita's
NikitasFeederConf = FeederConfiguration()
NikitasFeederConf.set_images_dir("/groups/ahrens/ahrenslab/Nikita/Realtime/imaging/test1_*")
NikitasFeederConf.set_behaviors_dir("/groups/ahrens/ahrenslab/Nikita/Realtime/ephys/")
NikitasFeederConf.set_spark_input_dir("/nobackup/freeman/streaminginput/")
NikitasFeederConf.set_timepoint_regexes(FeederConfiguration.RegexList(["TM(\d+)[_\.].*"]))
NikitasFeederConf.set_prefix_regexes(FeederConfiguration.RegexList(["behav   TM\d+\.10ch", "img TM\d+_.*\.stack"]))
NikitasFeederConf.set_mod_buffer_time(5)
NikitasFeederConf.set_max_files(-1)
NikitasFeederConf.set_check_size()

# Testing on cluster (Feeder output and Spark input set at the same time elsewhere)
ClusterTestingFeederConf = FeederConfiguration()
ClusterTestingFeederConf.set_images_dir("/groups/freeman/freemanlab/Streaming/demo_2015_02_20b/registered_im")
ClusterTestingFeederConf.set_behaviors_dir("/groups/freeman/freemanlab/Streaming/demo_2015_02_20b/registered_bv")
ClusterTestingFeederConf.set_max_files(40)
ClusterTestingFeederConf.set_image_prefix("images")
ClusterTestingFeederConf.set_behaviors_prefix("behaviour")
ClusterTestingFeederConf.set_poll_time(15.0)
ClusterTestingFeederConf.set_linger_time(300.0)

