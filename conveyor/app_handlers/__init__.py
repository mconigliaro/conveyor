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

        log.debug('Compared local version (%s) to requested version (%s) and got action: %s', local_version, requested_version, result)

        return result

    def get_version(self, id):
        log.critical('###### FIXME: running get_version ######')
        return '1.0'

    def upgrade(self, data):
        log.critical('###### FIXME: running upgrade ######')
        return True

    def downgrade(self, data):
        log.critical('###### FIXME: running downgrade ######')
        return True
