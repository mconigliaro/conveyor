from __future__ import absolute_import

from zookeeper import *

from .logging import log


PATH_SEPARATOR = '/'
ZOO_OPEN_ACL_UNSAFE = {"perms":PERM_ALL, "scheme":"world", "id":"anyone"};
PERSISTENT = 0


set_debug_level(0)


def zkjoin(list, absolute=False):
    """Construct a path from a list"""

    if absolute:
        result = PATH_SEPARATOR + PATH_SEPARATOR.join(list)
    else:
        result = PATH_SEPARATOR.join(list)

    return result


def zksplit(path):
    """Split a path into an array"""

    return filter(lambda i: i != '', path.split(PATH_SEPARATOR))


def get_parent_node(node):
    """Return the parent of the given node"""

    return zkjoin(zksplit(node)[0:-1], absolute=True)


def create_r(handle, path, data='', acl=[ZOO_OPEN_ACL_UNSAFE], create_mode=PERSISTENT):
    """Create nodes recursively"""

    while True:
        try:
            create(handle, path, data, acl, create_mode)
            log.debug('Created node: %s', path)
            break
        except NoNodeException:
            create_r(handle, get_parent_node(path), data='', acl=acl, create_mode=PERSISTENT)


def delete_r(handle, path):
    """Delete nodes recursively"""

    while True:
        try:
            delete(handle, path)
            log.debug('Deleted node: %s', path)
            break
        except NotEmptyException:
            for child in get_children(handle, path):
                delete_r(handle, zkjoin([path, child]))
