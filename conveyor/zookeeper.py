from __future__ import absolute_import

import logging

from zookeeper import *


PATH_SEPARATOR = '/'
ZOO_OPEN_ACL_UNSAFE = {"perms":PERM_ALL, "scheme":"world", "id":"anyone"};
PERSISTENT = 0


set_debug_level(0)

def path_join(*path_parts, **options):
    """Construct a path from a list of path parts"""

    result =  PATH_SEPARATOR.join(filter(lambda i: i != None and i != '', path_parts))

    if 'relative' not in options or not options['relative']:
        result = PATH_SEPARATOR + result

    return result


def path_split(path):
    """Split a path into an array"""

    return filter(lambda i: i != '', path.split(PATH_SEPARATOR))


def get_parent_node(node):
    """Return the parent of the given node"""

    return path_join(*path_split(node)[0:-1])


def create_r(handle, path, data='', acl=[ZOO_OPEN_ACL_UNSAFE], create_mode=PERSISTENT):
    """Create nodes recursively"""

    while True:
        try:
            create(handle, path, data, acl, create_mode)
            logging.getLogger().debug('Created node: %s', path)
            break
        except NoNodeException:
            create_r(handle, get_parent_node(path), data='', acl=acl, create_mode=PERSISTENT)


def delete_r(handle, path):
    """Delete nodes recursively"""

    while True:
        try:
            delete(handle, path)
            logging.getLogger().debug('Deleted node: %s', path)
            break
        except NotEmptyException:
            for child in get_children(handle, path):
                delete_r(handle, path_join(path, child, relative=True))
