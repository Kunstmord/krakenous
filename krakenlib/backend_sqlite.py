__author__ = 'George Oblapenko'
__license__ = "GPLv3"
"""
Everything is serialized and stored in string columns
"""

import sqlite3
from json import loads, dumps


def open_db(db_data: dict, writeback: bool=False) -> dict:
    """
    open the db for reading, return everything in a dict - cursor, connection, metadata
    """
    conn = sqlite3.connect(db_data['db_path'])
    return {'db': conn.cursor(),
            'db_connection': conn,
            'writeback': writeback, 'table_name': db_data['table_name']}


def close_db(db: dict):
    commit_db(db)
    db['db'].close()


def commit_db(db: dict):
    db['db_connection'].commit()
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
    return loads(db['db'].fetchone()[0])


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
    db['db'].execute('SELECT * FROM :tablename WHERE id=:record_id', {'tablename': db['table_name'],
                                                                      'record_id': record_id})

    res = db['db'].fetchone()
    return list(map(lambda x: loads(x), res[1:]))


def data_exists(db: dict, record_id: int, data_name: str) -> bool:
    """Check if data exists for a given column name and record_id
    """
    db['db'].execute('SELECT :data_name FROM :tablename WHERE id=:record_id', {'data_name': data_name,
                                                                               'tablename': db['table_name'],
                                                                               'record_id': record_id})
    if not db['db'].fetchone()[0]:
        return False
    else:
        return True


def all_data_names(db: dict) -> list:
    """Return data column names
    """
    db['db'].execute('SELECT * FROM :tablename', {'tablename': db['table_name']})
    return list(map(lambda x: x[0], db['db'].description))


def delete_record(db: dict, record_id: int, total_records):
    """Delete a record specified by record_id
    """
    db['db'].execute('DELETE FROM :tablename WHERE id=:record_id', {'tablename': db['table_name'],
                                                                    'record_id': record_id})

    # for i in range(record_id, total_records):
    #     db['db'][str(record_id)] = db['db'][record_id + 1]
    del db['db'][str(total_records)]


def data_records_amount(db_data: dict) -> int:
    """How many data records are there in all?
    """
    conn = sqlite3.connect(db_data['db_path'])
    c = conn.cursor()
    c.execute('SELECT Count(*) FROM :tablename', {'tablename': db_data['table_name']})
    return c.fetchone()[0]
