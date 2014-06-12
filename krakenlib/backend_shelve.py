__author__ = 'George Oblapenko'
__license__ = "GPLv3"
"""
The structure is {str(id): {'feature1': feature1, ...}, ...}
"""

import shelve
from numpy import ndarray
from krakenlib.misc import is_a_number


def open_db(db_data: dict, writeback: bool=False) -> dict:
    """
    open the db for reading, return everything in a dict
    """

    return {'db': shelve.open(db_data['shelve_path'], writeback=writeback)}


def close_db(db: dict):
    commit_db(db)
    db['db'].close()


def commit_db(db: dict):
    db['db'].sync()


def write_data(db: dict, record_id: int, data_name: str, data):
    if not db['db'].keys():
        db['db'][str(record_id)] = {}
    elif str(record_id) not in db['db']:
        db['db'][str(record_id)] = {}
    if not db['db'][str(record_id)].keys():
        db['db'][str(record_id)] = {data_name: data}
    else:
        if db['db'].__getattribute__('writeback') is True:
            db['db'][str(record_id)][data_name] = data
        else:
            tmp_dict = db['db'][str(record_id)]
            tmp_dict[data_name] = data
            db['db'][str(record_id)] = tmp_dict


def read_data(db: dict, record_id: int, data_name: str):
    return db['db'][str(record_id)][data_name]


def read_all_data(db: dict, record_id: int):
    return_dict = {}
    for data_name in db['db'][str(record_id)]:
        return_dict[data_name] = db['db'][str(record_id)][data_name]
    return return_dict


def delete_data(db: dict, record_id: int, data_name: str):
    if data_name in db['db'][str(record_id)].keys():  # we check whether the column exists for the current record
        if db['db'].__getattribute__('writeback') is True:
            del db['db'][str(record_id)][data_name]
        else:
            tmp_dict = db['db'][str(record_id)]
            del tmp_dict[data_name]
            db['db'][str(record_id)] = tmp_dict
    # del db['db'][str(record_id)][data_name]


def feature_exists(db: dict, record_id: int, feature_name: str) -> bool:
    if db['db'][str(record_id)] == {}:
        return False
    elif feature_name not in db['db'][str(record_id)].keys():
        return False
    else:
        return True


def feature_names(db: dict, record_id: int) -> list:
    if db['db'][str(record_id)] == {}:
        return []
    else:
        return_list = []
        for data_name in db['db'][str(record_id)]:
            feature_len = -1
            if isinstance(db['db'][str(record_id)][data_name], ndarray):
                np_size = 1
                for i in db['db'][str(record_id)][data_name].shape:
                    np_size *= i
                feature_len = np_size
            elif isinstance(db['db'][str(record_id)][data_name], list):
                feature_len = len(db['db'][str(record_id)][data_name])
            elif is_a_number(db['db'][str(record_id)][data_name]):
                feature_len = 0
            elif isinstance(db['db'][str(record_id)][data_name], str):
                feature_len = 'string'
            return_list.append((data_name, feature_len))
        return return_list


def data_records_amount(db_data: dict) -> int:
    db = shelve.open(db_data['shelve_path'])
    records_amount = len(db.keys())
    db.close()
    return records_amount