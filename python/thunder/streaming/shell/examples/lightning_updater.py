from thunder.streaming.shell.updater import Updater
import json

class LightningUpdater(Updater):

    def __init__(self, tssc, viz, tag):
        Updater.__init__(self, tssc, pause=2)
        self.viz = viz
        self.tag = tag

    def fetch_update(self):
        regions = self.viz.get_coords(return_type='points')
        print "regions: %s, len: %d" % (str(regions), len(regions))
        return self.tag, [r.tolist() for r in regions]
