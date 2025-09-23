def rm_duplicates(rdd):
    """
    Rm duplicates, keep 1st row only per detection_oid
    """

    def to_key_value(row):
        # Create key-value pair: [(detection_oid, {row})]
        return (row['detection_oid'], row)

    def keep_first_value(first_row, second_row):
        # Keep 1st row
        return first_row

    def get_only_value(pair):
        # Rm key: [({row})]
        return pair[1]

    # Pair by detection_oid
    keyed_rdd = rdd.map(to_key_value)

    # Rm duplicates, keep 1st row only
    rm_dup_rdd = keyed_rdd.reduceByKey(keep_first_value)

    # Rm key, get back row
    return rm_dup_rdd.map(get_only_value)


def map_to_item_count(row):
    """
    Convert row into a countable pair
    [((101, "Car"), 1)
    ((102, "Bike"), 1)]
    """

    key = (row['geographical_location_oid'], row['item_name'])
    return (key, 1)


def reduce_count(count1, count2):
    """
    Add 2 rows with count together
    [
    ((101, "car",),1),
    ((101, "car"), 1)
    ]
    """
    return count1 + count2


def restructure_count(record):
    """
    Convert ((location_id, item_name), count)
    to (location_id, (item_name, count))
    ((101, "car"), 1) -> (101, ("car", 1))
    Using location_id as main key
    """

    key = record[0]      # (location_id, item_name)
    count = record[1]    # total count
    location_id = key[0]
    item_name = key[1]

    return (location_id, (item_name, count))


def group_and_get_top_items(location_items_rdd, top_x):

    """
    Group items by location, compute TOP_X per location.
    """

    """
    ???
    """

    return result




def map_location_dict(row):
    """
    Convert dataset B rows into (location_oid, location_name)
    {geographical_location_oid:101, "geographical_location":"onenorth"}
    """
    return (row['geographical_location_oid'], row['geographical_location'])


def attach_location_name(record, broadcast_loc):
    """
    Add location name to the ranked results.
    Input: (location_id, rank, item_name): (101, 1, "car")
    Output: (location_id, location_name, rank, item_name): (101, "onenorth", 1, "Car")
    """
    loc_id = record[0]
    rank = record[1]
    item_name = record[2]

    loc_name = broadcast_loc.value.get(loc_id, "Unknown")

    return (loc_id, loc_name, rank, item_name)
