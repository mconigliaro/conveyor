from __future__ import absolute_import

import string


def comma_str_to_list(groups):
    """Convert a comma separated list of things into a unique list"""

    return list(set(map(string.strip, groups.split(','))))


def read_options(*sources, **options):
    """Read options from a list of sources (last source wins)"""

    data = {}

    for source in sources:
        for name, value in source:
            name = string.replace(name, '-', '_') # FIXME: this may become a source of weird bugs...
            if value:
                if name in options['to_list']:
                    data[name] = comma_str_to_list(value)
                else:
                    data[name] = value

    return data
