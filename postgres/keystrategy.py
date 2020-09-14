
MIN_INDEXES = 1


def primary_keys(keys):
    return list(filter(lambda r: r.get('indisprimary'), keys))


def unique_keys(keys, unique=True):
    return indexes(keys, unique)


def non_unique_keys(keys, unique=False):
    return indexes(keys, unique)


def indexes(keys, unique):
    keys = list(filter(lambda r: r.get('indisunique') == unique, keys))

    # Get indexes with multiple columns
    multiple = list(filter(lambda r: r.get('indnatts') > MIN_INDEXES, keys))

    if multiple:
        # Get first column and find the other columns for this index
        first = multiple[0].get('indexrelid')

        return list(filter(lambda r: r.get('indexrelid') == first, multiple))

    # Return first index
    return keys[:1]


KEY_STRATEGY = [
    primary_keys,
    unique_keys,
    non_unique_keys
]
