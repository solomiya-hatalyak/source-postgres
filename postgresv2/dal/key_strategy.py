from copy import copy


def primary_keys(keys: list):
    return [element for element in keys if element['indisprimary']]


def unique_keys(keys: list):
    return get_index(keys, unique=True)


def non_unique_keys(keys: list):
    return get_index(keys, unique=False)


def get_index(keys: list, unique: bool) -> list:
    keys = [element for element in keys if element['indisunique'] == unique]
    if not keys:
        return

    # Get first column and find the other columns for this index
    first = keys[0]
    multiple = first['indnatts'] > 1

    if multiple:
        # Find other columns for this index
        return [element for element in keys
                if element['indexrelid'] == first['indexrelid']]

    return [first]


KEY_STRATEGY = [
    primary_keys,
    unique_keys,
    non_unique_keys
]


def choose_index(keys: list) -> list:
    keys_copy = copy(keys)

    for strategy in KEY_STRATEGY:
        results = strategy(keys_copy)

        if results:
            ordered_index = sorted(results, key=lambda i: i['column_order'])
            return [element['attname'] for element in ordered_index]
    return []
