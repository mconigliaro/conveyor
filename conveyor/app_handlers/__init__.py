from __future__ import absolute_import

from ..logging import log


class Default():

    def __init__(self, conveyor):
        self.conveyor = conveyor
        log.debug('Initialized app handler')

    def run(self, data):
        log.critical('###### FIXME: do something ######')
        log.critical(data)
