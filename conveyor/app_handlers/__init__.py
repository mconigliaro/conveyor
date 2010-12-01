from __future__ import absolute_import

from distutils import version

from ..logging import log


class Default():
    """Base class for all app handlers"""

    def __init__(self):
        log.debug('Initialized app handler: %s', self.__class__)

    def get_action(self, app):
        """Select the appropriate action"""

        local_version = version.StrictVersion(str(self.get_version(id=app.id)))
        requested_version = version.StrictVersion(str(app.data['version']))

        if local_version < requested_version:
            result = self.upgrade
        elif local_version > requested_version:
            result = self.downgrade
        else:
            result = None

        log.info('app=%s, lversion=%s, rversion=%s, action=%s', app.id, local_version, requested_version, result.__name__)

        return result

    def get_version(self, id):
        return '0.0'

    def upgrade(self, data):
        return True

    def downgrade(self, data):
        return True
