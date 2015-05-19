import settings
from threading import Thread
import os
import time

class FileMonitor(Thread):
    """
    Monitors an Analysis' output directory and periodically sends the entire output from a single timepoint to all
    converters registered on that Analysis
    """

    # Period with which the FileMonitor will check for new data directories
    DIR_POLL_PERIOD = 1
    # Period with which the FileMonitor will check monitored directories for new files
    FILE_POLL_PERIOD = 10
    # Time between each poll
    WAIT_PERIOD = 0.5

    monitors = {} 

    @staticmethod
    def getInstance(analysis): 
        monitor = FileMonitor.monitors.get(analysis.output_dir)
        if not monitor: 
            monitor = FileMonitor(analysis.output_dir)
            FileMonitor.monitors[analysis.output_dir] = monitor
        monitor.add_analysis(analysis)
        return monitor

    @staticmethod
    def start(): 
        for monitor in FileMonitor.file_monitors: 
            monitor.start() 

    @staticmethod
    def stop(): 
        for monitor in FileMonitors.file_monitors: 
            monitor.stop() 

    def __init__(self, output_dir):
        Thread.__init__(self)
        self.output_dir = output_dir
        self.analyses = []
        self._stopped = False

        # Fields involved in directory monitoring
        self._last_dir_state = None
        cur_time = time.time()
        self.last_dir_poll = cur_time

        # A dict of 'dir_name' -> (last_state, last_mod_time)
        self.monitored_dirs = {}

    def stop(self):
        self._stopped = True

    def add_analysis(self, analysis): 
        self.analyses.append(analysis)

    def _start_monitoring(self, diff):
        for dir in diff:
            self.monitored_dirs[dir] = (diff, time.time())

    def _qualified_dir_set(self, root):
        if not root:
            return None
        return set([os.path.join(root, f) for f in os.listdir(root)])

    def _qualified_file_set(self, root):
        def path_and_size(f):
            path = os.path.join(root, f)
            size = os.path.getsize(path)
            return (path, size)
        if not root:
            return None
        return set([path_and_size(f) for f in os.listdir(root)])

    def _load_files(self, names): 
        data = {} 
        for name in names: 
            data[name] = open(name, 'rb').read() 
        return data 
        
    def run(self):
        while not self._stopped:
            cur_time = time.time()
            dp = cur_time - self.last_dir_poll
            if dp > self.DIR_POLL_PERIOD:
                cur_dir_state = self._qualified_dir_set(self.output_dir)
                if cur_dir_state != self._last_dir_state:
                    if self._last_dir_state != None:
                        diff = cur_dir_state.difference(self._last_dir_state)
                        self._start_monitoring(diff)
                    self._last_dir_state = cur_dir_state
                self.last_dir_poll = cur_time
            for dir, info in self.monitored_dirs.items():
                dir_state = self._qualified_file_set(dir)
                if info[0] != dir_state:
                    self.monitored_dirs[dir] = (dir_state, time.time())
                elif info[0]:
                    # Only want to get to this point if the directory is not empty
                    if (time.time() - info[1]) > self.FILE_POLL_PERIOD:
                        # The directory has remained the same for a sufficient period of time
                        names = map(lambda x: x[0], dir_state)
                        # Only load the data once per monitored directory
                        loaded_data = self._load_files(names)
                        for analysis in self.analyses: 
                            analysis.handle_new_data(loaded_data) 
                        del self.monitored_dirs[dir]
            time.sleep(self.WAIT_PERIOD)


