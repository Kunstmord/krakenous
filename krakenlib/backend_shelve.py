__author__ = 'George Oblapenko'
__license__ = "GPLv3"
"""
The structure is {id: {'feature1': feature1, ...}, ...}
"""

import shelve


def open_db(db_data: dict, writeback: bool=False) -> dict:
    """
    open the db for reading, return everything in a dict
    """
    return {'db': shelve.open(db_data['db_path'], writeback=writeback),
            'writeback': writeback}


def close_db(db: dict):
    commit_db(db)
    db['db'].close()


def commit_db(db: dict):
    db['db'].sync()


def write_data(db: dict, record_id: int, data_name: str, data):
    """Write data for a given record_id
    """

    if not db['db'].keys():
        db['db'][str(record_id)] = {}
    elif str(record_id) not in db['db']:
        db['db'][str(record_id)] = {}
    if not db['db'][str(record_id)].keys():
        db['db'][str(record_id)] = {data_name: data}
    else:
        if db['writeback']:
            db['db'][str(record_id)][data_name] = data
        else:
            tmp_dict = db['db'][str(record_id)]
            tmp_dict[data_name] = data
            db['db'][str(record_id)] = tmp_dict


def append_data_record(db: dict, record_id: int, data_dict: dict):
    """append a record with id=record_id
    """

    for data_entry in data_dict:
        write_data(db, record_id, data_entry, data_dict[data_entry])


def read_single_data(db: dict, record_id: int, data_name: str):
    """Return the contents of a data column specified by data_name for a given record_id
    """
    if data_name in db['db'][str(record_id)].keys():
        return db['db'][str(record_id)][data_name]
    else:
        return None


def read_multiple_data(db: dict, record_id: int, data_names: tuple):
    """Return the contents of data columns specified by contents data_names for a given record_id
    """
    result = {}
    for data_name in data_names:
        if data_name in db['db'][str(record_id)].keys():
            result[data_name] = db['db'][str(record_id)][data_name]
    return result


def read_all_data(db: dict, record_id: int):
    """
    Return the contents of all data columns for a given record_id
    """
    return_dict = {}
    for data_name in db['db'][str(record_id)]:
        return_dict[data_name] = db['db'][str(record_id)][data_name]
    return return_dict


def delete_data(db: dict, record_id: int, data_name: str):
    """Delete data for a given column name and record_id
    """
    if data_name in db['db'][str(record_id)].keys():
        if db['writeback']:
            del db['db'][str(record_id)][data_name]
        else:
            tmp_dict = db['db'][str(record_id)]
            del tmp_dict[data_name]
            db['db'][str(record_id)] = tmp_dict


def data_exists(db: dict, record_id: int, data_name: str) -> bool:
    """Check if data exists for a given column name and record_id
    """
    if db['db'][str(record_id)] == {}:
        return False
    elif data_name not in db['db'][str(record_id)].keys():
        return False
    else:
        return True


def all_data_names(db: dict, record_id: int) -> list:
    """Return data names for a given record
    """
    if db['db'][str(record_id)] == {}:
        return []
    else:
        return_list = []
        for data_name in db['db'][str(record_id)]:
            return_list.append(data_name)
        return return_list


def delete_record(db: dict, record_id: int, total_records):
    """Delete a record specified by record_id
    """
    for i in range(record_id, total_records):
        db['db'][str(record_id)] = db['db'][record_id + 1]
    del db['db'][str(total_records)]


def data_records_amount(db_data: dict) -> int:
    """How many data records are there in all?
    """
    db = shelve.open(db_data['db_path'])
    records_amount = len(db.keys())
    db.close()
    return records_amount
