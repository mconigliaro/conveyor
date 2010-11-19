from __future__ import absolute_import

import conveyor


class Default():

    def __init__(self, groups=list()):
        self.groups = groups

        conveyor.log.critical('###### FIXME: init ######')

    def fixme(self, data):
        conveyor.log.critical('###### FIXME: do something ######')
        conveyor.log.critical(data)
