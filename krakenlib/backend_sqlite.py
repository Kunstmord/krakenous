__author__ = 'George Oblapenko'
__license__ = "GPLv3"
"""
Everything is serialized and stored in string columns
"""

import sqlite3
from json import loads, dumps


def open_db(db_data: dict, writeback: bool=False) -> dict:
    """
    open the db for reading, return everything in a dict
    """
    return {'db': sqlite3.connect(db_data['db_path']).cursor(),
            'writeback': writeback, 'table_name': db_data['table_name']}


def close_db(db: dict):
    commit_db(db)
    db['db'].close()


def commit_db(db: dict):
    db['db'].commit()
#
#
# def write_data(db: dict, record_id: int, data_name: str, data):
#     """Write data for a given record_id
#     """
#
#     if not db['db'].keys():
#         db['db'][str(record_id)] = {}
#     elif str(record_id) not in db['db']:
#         db['db'][str(record_id)] = {}
#     if not db['db'][str(record_id)].keys():
#         db['db'][str(record_id)] = {data_name: data}
#     else:
#         if db['writeback']:
#             db['db'][str(record_id)][data_name] = data
#         else:
#             tmp_dict = db['db'][str(record_id)]
#             tmp_dict[data_name] = data
#             db['db'][str(record_id)] = tmp_dict


def read_single_data(db: dict, record_id: int, data_name: str):
    """Return the contents of a data column specified by data_name for a given record_id
    """
    db['db'].execute('SELECT :data_name FROM :tablename WHERE id=:record_id', {'data_name': data_name,
                                                                                     'tablename': db['table_name'],
                                                                                     'record_id': record_id})
    return loads(tuple(db['db'].fetchone())[0])
    # if data_name in db['db'][str(record_id)].keys():
    #     return db['db'][str(record_id)][data_name]
    # else:
    #     return None


def read_multiple_data(db: dict, record_id: int, data_names: tuple):
    """Return the contents of data columns specified by contents data_names for a given record_id
    """
    result = {}
    for data_name in data_names:
        result[data_name] = read_single_data(db, record_id, data_name)
    return result


def read_all_data(db: dict, record_id: int):
    """
    Return the contents of all data columns for a given record_id
    """
    # return_dict = {}
    # for data_name in db['db'][str(record_id)]:
    #     return_dict[data_name] = db['db'][str(record_id)][data_name]
    # return return_dict
    result = {}
    db['db'].execute('SELECT * FROM :tablename WHERE id=:record_id', {'tablename': db['table_name'],
                                                                      'record_id': record_id})

    res = db['db'].fetchone()
    res_keys = res.keys()
    for key in res_keys:
        result[key] = loads(res[key])
    return result
    # return loads(res.fetchone())
#
#
# def delete_data(db: dict, record_id: int, data_name: str):
#     """Delete data for a given column name and record_id
#     """
#     if data_name in db['db'][str(record_id)].keys():
#         if db['writeback']:
#             del db['db'][str(record_id)][data_name]
#         else:
#             tmp_dict = db['db'][str(record_id)]
#             del tmp_dict[data_name]
#             db['db'][str(record_id)] = tmp_dict
#
#
# def data_exists(db: dict, record_id: int, data_name: str) -> bool:
#     """Check if data exists for a given column name and record_id
#     """
#     if db['db'][str(record_id)] == {}:
#         return False
#     elif data_name not in db['db'][str(record_id)].keys():
#         return False
#     else:
#         return True
#
#

def all_data_names(db: dict) -> list:
    """Return data column names
    """
    db['db'].execute('SELECT * FROM :tablename', {'tablename': db['table_name']})

    res = db['db'].fetchone()
    res_keys = res.keys()
    return res_keys
    # if db['db'][str(record_id)] == {}:
    #     return []
    # else:
    #     return_list = []
    #     for data_name in db['db'][str(record_id)]:
    #         return_list.append(data_name)
    #     return return_list
#
#
# def delete_record(db: dict, record_id: int, total_records):
#     """Delete a record specified by record_id
#     """
#     for i in range(record_id, total_records):
#         db['db'][str(record_id)] = db['db'][record_id + 1]
#     del db['db'][str(total_records)]
#
#
# def data_records_amount(db_data: dict) -> int:
#     """How many data records are there in all?
#     """
#     db = shelve.open(db_data['shelve_path'])
#     records_amount = len(db.keys())
#     db.close()
#     return records_amount
