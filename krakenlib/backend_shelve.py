__author__ = 'George Oblapenko'
__license__ = "GPLv3"
"""
The structure is {str(id): {'feature1': feature1, ...}, ...}
"""

import shelve


def open_db(db_data: dict, writeback: bool=False) -> dict:
    """
    open the db for reading, return everything in a dict
    """

    return {'db': shelve.open(db_data['shelve_path'], writeback=writeback)}


def close_db(db_dict: dict):
    db_dict['db'].close()


def commit_db(db_dict: dict):
    db_dict['db'].sync()


def write_data(db, record_id: int, data_name: str, data):
    db['db'][str(record_id)][data_name] = data


def read_data(db, record_id: int, data_name: str):
    return db['db'][str(record_id)][data_name]


def delete_data(db, record_id: int, data_name: str):
    del db['db'][str(record_id)][data_name]


def delete_feature(db, feature_name: str):
    for data_record in db['db']:
        del data_record[feature_name]


def exists_feature(db, record_id: int, feature_name: str) -> bool:
    if db['db'][str(record_id)] == {}:
        return False
    elif feature_name not in db['db'][str(record_id)].values():
        return False
    else:
        return True