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
    commit_db(db_dict)
    db_dict['db'].close()


def commit_db(db_dict: dict):
    db_dict['db'].sync()


def write_data(db, record_id: int, data_name: str, data):
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


def read_data(db, record_id: int, data_name: str):
    return db['db'][str(record_id)][data_name]


def read_all_data(db, record_id: int):
    return_dict = {}
    for data_name in db['db'][str(record_id)]:
        return_dict[data_name] = db['db'][str(record_id)][data_name]
    return return_dict


def delete_data(db, record_id: int, data_name: str):
    del db['db'][str(record_id)][data_name]


def delete_feature(db, feature_name: str):
    for data_record in db['db']:
        del data_record[feature_name]


def feature_exists(db, record_id: int, feature_name: str) -> bool:
    if db['db'][str(record_id)] == {}:
        return False
    elif feature_name not in db['db'][str(record_id)].keys():
        return False
    else:
        return True
