__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
"""
Various pre-rolled functions to ease common operations - extracting data from csv files or files in a folder,
sync/dump/copy datasets
"""
from csv import reader
from os.path import join, isfile
import numpy as np
from os import walk
from krakenous.dataset import DataSet
from krakenous.misc import element_length
from krakenous.errors import KrakenousException
from json import dumps, loads


def folder_tentacle(path_to_folder, column_name: str, exclude_files_list: tuple=(), filename_transform=None,
                    keep_original_filename: bool=True, original_filename_column_name: str='filename', **kwargs):
    for (dirpath, dirnames, filenames) in walk(path_to_folder):
        for f_name in filenames:
            if not filename_transform:
                if f_name not in exclude_files_list:
                    yield {column_name: f_name}
            else:
                if f_name not in exclude_files_list:
                    if keep_original_filename:
                        yield {original_filename_column_name: f_name, column_name: filename_transform(f_name, **kwargs)}
                    else:
                        yield {column_name: filename_transform(f_name, **kwargs)}


def add_labels(dataset: DataSet, id_field_name: str, labels, labels_name: str='label', overwrite_existing: bool=False):
    """
    labels - iterable of iterables - ((some_id, label), ...)
    """
    if not overwrite_existing and dataset.feature_exists_global(labels_name):
        raise KrakenousException('A field with name ' + labels_name + ' already exists')
    if len(labels) != dataset.total_records:
        raise KrakenousException('Number of labels is different from the number of records in the dataset')
    for data_record in dataset.yield_data_records(('id', id_field_name)):
        for single_label in labels:
            if data_record['id_field_name'] in single_label[0]:
                dataset.insert_single(data_record['id'], labels_name, single_label[1], True)


def dump_dataset(origin_dataset: DataSet, new_dataset: DataSet, column_names: tuple=(),
                 serializers=None):
    #TODO - SQL -> shelve then serializers deserialize from strings to Python Objects
    """
    if shelve -> SQL then serializers serialize from Python objects to strings
    if SQL -> shelve then serializers deserialize from strings to Python Objects
    """
    for data_record in origin_dataset.yield_data_records(column_names):
        del data_record['id']
        new_dataset.append_data_record(data_record, serializers)


def sync_datasets(dataset1: DataSet, dataset2: DataSet, serializers: dict=None, deserializers: dict=None):
    #TODO what if key doesn't exist in case of the shelve backend?

    if dataset1.total_records != dataset2.total_records:
        raise KrakenousException('Different numbers of records in datasets')
    data_names1 = dataset1.feature_names()
    data_names2 = dataset2.feature_names()
    from_first_to_second = ['id']
    from_second_to_first = ['id']
    for data_name in data_names1:
        if data_name not in data_names2:
            from_first_to_second.append(data_name)
    for data_name in data_names2:
        if data_name not in data_names1:
            from_second_to_first.append(data_name)
    for record in dataset1.yield_data_records(from_first_to_second, deserializers=deserializers):
        for data_name in from_first_to_second:
            if data_name != 'id':
                if serializers and data_name in serializers:
                    dataset2.insert_single(record['id'], data_name, record[data_name],
                                           serializer=serializers[data_name])
                else:
                    dataset2.insert_single(record['id'], data_name, record[data_name])
    for record in dataset2.yield_data_records(from_second_to_first, deserializers=deserializers):
        for data_name in from_second_to_first:
            if data_name != 'id':
                if serializers and data_name in serializers:
                    dataset1.insert_single(record['id'], data_name, record[data_name],
                                           serializer=serializers[data_name])
                else:
                    dataset1.insert_single(record['id'], data_name, record[data_name])


def convert_single_feature_numpy(dataset: DataSet, feature_name: str, start_id: int=1, end_id: int=-1):
    # always returns a 2d array
    if dataset.total_records == 0:
        raise KrakenousException('The dataset is empty')
    if end_id == -1:
        end_id = dataset.total_records
    else:
        if end_id > dataset.total_records:
            raise KrakenousException('Incorrect range, end_id=' + str(end_id), ', while total_records='
                                     + str(dataset.total_records))
    if start_id < 0 or start_id > end_id:
        raise KrakenousException('start_id cannot be less than 0 or bigger than end_id')

    starting = dataset.single_data_record(start_id, (feature_name,))

    el_length = element_length(starting[feature_name])
    if el_length[1] == 'string':
        raise KrakenousException('Cannot convert column ' + feature_name + ' to numpy array')
    result = np.zeros((end_id - start_id + 1, el_length[0]))
    if el_length[1] == 'number' or el_length[1] == 'tuple' or el_length[1] == 'list':
        for data_record in enumerate(dataset.yield_data_records((feature_name,), start_id, end_id)):
            result[data_record[0], :] = data_record[1][feature_name]
    elif el_length[1] == 'ndarray':
        for data_record in enumerate(dataset.yield_data_records((feature_name,), start_id, end_id)):
            result[data_record[0], :] = data_record[1][feature_name].flatten()
    return result


def convert_multiple_features_numpy(dataset: DataSet, feature_names: tuple, start_id: int=1, end_id: int=-1):

    if dataset.total_records == 0:
        raise KrakenousException('The dataset is empty!')
    if end_id == -1:
        end_id = dataset.total_records
    else:
        if end_id > dataset.total_records:
            raise KrakenousException('Incorrect range, end_id=' + str(end_id), ', while total_records='
                                     + str(dataset.total_records))
    if start_id < 0 or start_id > end_id:
        raise KrakenousException('start_id cannot be less than 0 or bigger than end_id')

    total_length = 0

    starting = dataset.single_data_record(start_id, feature_names)

    lengths = []
    for feature_name in feature_names:
        feature_len = element_length(starting[feature_name])
        if feature_len[1] == 'string' or feature_len[0] <= 0:
            raise KrakenousException('Cannot convert column ' + feature_name + ' to numpy array')
        else:
            total_length += feature_len[0]
            lengths.append(feature_len)
    if total_length > 0:
        result = np.zeros((end_id + 1 - start_id, total_length))
        for data_record in enumerate(dataset.yield_data_records(feature_names, start_id, end_id)):
            curr_len = 0
            for name_number_pair in enumerate(feature_names):
                el_type = lengths[name_number_pair[0]][1]
                el_len = lengths[name_number_pair[0]][0]
                if el_type == 'number' or el_type == 'tuple' or el_type == 'list':
                    result[data_record[0],
                           curr_len:curr_len + el_len] = data_record[1][name_number_pair[1]]
                    curr_len += el_len
                elif el_type == 'ndarray':
                    result[data_record[0],
                           curr_len:curr_len + el_len] = data_record[1][name_number_pair[1]].flatten()
                    curr_len += el_len
        return result
    else:
        raise KrakenousException('No features that can be fit into a numpy array found in the passed tuple')


def numpy_array_deserializer(string: list) -> np.ndarray:
    return np.asarray(loads(string))


def numpy_array_serializer(array: np.ndarray) -> str:
    return dumps(array.tolist())