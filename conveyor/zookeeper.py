from __future__ import absolute_import

from zookeeper import *

from .logging import log


PATH_SEPARATOR = '/'
ZOO_OPEN_ACL_UNSAFE = {"perms":PERM_ALL, "scheme":"world", "id":"anyone"};
PERSISTENT = 0


set_debug_level(0)


def get_parent_node(node, seperator=PATH_SEPARATOR):
    """Return the parent of the given node"""

    return seperator.join(node.split(seperator)[0:-1])


def create_r(handle, path, data, acl=[ZOO_OPEN_ACL_UNSAFE], create_mode=PERSISTENT):
    """Create nodes recursively"""

    while True:
        try:
            create(handle, path, data, acl, create_mode)
            log.debug('Created node: %s', path)
            break
        except NoNodeException:
            create_r(handle, get_parent_node(path), data, acl, create_mode)


def delete_r(handle, path):
    """Delete nodes recursively"""

    while True:
        try:
            delete(handle, path)
            log.debug('Deleted node: %s', path)
            break
        except NotEmptyException:
            for child in get_children(handle, path):
                delete_r(handle, PATH_SEPARATOR.join([path, child]))
