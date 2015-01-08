__author__ = 'George Oblapenko'
__license__ = "GPLv3"
"""
Everything is serialized and stored in string columns
"""

import sqlite3
from json import loads, dumps


def create_db(db_data: dict):
    conn = sqlite3.connect(db_data['db_path'])
    c = conn.cursor()
    sql_string = 'CREATE TABLE ' + db_data['table_name'] + ' (id integer primary key)'
    c.execute(sql_string)
    conn.commit()
    conn.close()


def open_db(db_data: dict) -> dict:
    """
    open the db for reading, return everything in a dict - cursor, connection, metadata
    """
    conn = sqlite3.connect(db_data['db_path'])
    return {'db': conn.cursor(),
            'db_connection': conn, 'table_name': db_data['table_name']}


def close_db(db: dict):
    commit_db(db)
    db['db'].close()


def commit_db(db: dict):
    db['db_connection'].commit()


def create_new_column(db: dict, data_name: str):
    sql_string = 'ALTER TABLE ' + db['table_name'] + ' ADD COLUMN ' + data_name + ' TEXT'
    db['db'].execute(sql_string)


def write_data(db: dict, record_id: int, data_name: str, data, serializer):
    sql_string = 'UPDATE ' + db['table_name'] + ' SET ' + data_name + '=? WHERE id=?'
    db['db'].execute(sql_string, (serializer(data), record_id))


def append_data_record(db: dict, record_id: int, data_dict: dict, serializers=None):
    c_names = '('
    c_vals = '('
    insertion_data = []
    for k in data_dict:
        if serializers and k in serializers:
            insertion_data.append(serializers[k](data_dict[k]))
        else:
            insertion_data.append(dumps(data_dict[k]))
        c_names += k + ', '
        c_vals += '?, '
    c_names = c_names[:-2] + ')'
    c_vals = c_vals[:-2] + ')'
    sql_string = 'INSERT INTO ' + db['table_name'] + c_names + ' VALUES ' + c_vals
    db['db'].execute(sql_string, tuple(insertion_data))


def read_single_data(db: dict, record_id: int, data_name: str, deserializer=None, filters=None):
    """Return the contents of a data column specified by data_name for a given record_id
    """
    sql_string = 'SELECT ' + data_name + ' FROM ' + db['table_name'] + ' WHERE id=' + str(record_id)

    if filters:
        for filter_key in filters:
            sql_string += ' AND ' + filter_key + '=' + str(filters[filter_key])
    db['db'].execute(sql_string)
    res = db['db'].fetchone()
    if res is not None:
        if deserializer:
            return deserializer(res[0])
        else:
            return loads(res[0])
    else:
        return None


def read_multiple_data(db: dict, record_id: int, data_names: tuple, deserializers=None):
    """Return the contents of data columns specified by contents data_names for a given record_id
    """
    result = {}
    for data_name in data_names:
        if data_name in deserializers:
            result[data_name] = read_single_data(db, record_id, data_name, deserializers[data_name])
        else:
            result[data_name] = read_single_data(db, record_id, data_name)
    return result


def read_all_data(db: dict, record_id: int, deserializers=None):
    """
    Return the contents of all data columns for a given record_id
    """
    sql_string = 'SELECT * FROM ' + db['table_name'] + ' WHERE id=' + str(record_id)
    db['db'].execute(sql_string)
    res = db['db'].fetchone()

    if deserializers:
        result = []
        for x_enum in enumerate(res[1:]):
            if x_enum[0] in deserializers:
                result.append(deserializers[x_enum[0]](x_enum[1]))
            else:
                result.append(loads(x_enum[1]))
    else:
        result = list(map(lambda x: loads(x), res[1:]))
    return result


def data_exists(db: dict, record_id: int, data_name: str) -> bool:
    """Check if data exists for a given column name and record_id
    """
    sql_string = 'SELECT ' + data_name + ' FROM ' + db['table_name'] + ' WHERE id=' + str(record_id)
    db['db'].execute(sql_string)
    if not db['db'].fetchone()[0]:
        return False
    else:
        return True


def all_data_names(db: dict) -> list:
    """Return data column names (doesn't include id)
    """
    sql_string = 'SELECT * FROM ' + db['table_name']
    db['db'].execute(sql_string)
    return list(map(lambda x: x[0], db['db'].description))[1:]


# def delete_record(db: dict, record_id: int, total_records):
#     """Delete a record specified by record_id
#     """
#     db['db'].execute('DELETE FROM :tablename WHERE id=:record_id', {'tablename': db['table_name'],
#                                                                     'record_id': record_id})
#
#     # for i in range(record_id, total_records):
#     #     db['db'][str(record_id)] = db['db'][record_id + 1]
#     del db['db'][str(total_records)]


def data_records_amount(db_data: dict) -> int:
    """How many data records are there in all?
    """
    conn = sqlite3.connect(db_data['db_path'])
    c = conn.cursor()
    sql_string = 'SELECT Count(*) FROM ' + db_data['table_name']
    c.execute(sql_string)
    res = c.fetchone()[0]
    conn.close()
    return res
