from __future__ import absolute_import

import string


def comma_str_to_list(str_list):
    """Convert a comma separated list of things into a unique list"""

    try:
        result = sorted(list(set(map(string.strip, str_list.split(',')))))
    except AttributeError:
        result = []

    return result


def read_options(*sources, **options):
    """Read options from a list of sources (last source wins)"""

    data = {}

    for source in sources:

        if source.__class__ == dict:
            source = source.items()

        for name, value in source:
            name = string.replace(name, '-', '_') # FIXME: This may become a source of difficult to track down bugs...
            if value:
                if 'to_list' in options and name in options['to_list']:
                    data[name] = comma_str_to_list(value)
                else:
                    data[name] = value

    return data
