__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
"""
Various pre-rolled functions to ease common operations - extracting data from csv files or files in a folder,
sync/dump/copy datasets
"""
from csv import reader
from os.path import join, isfile
from os import walk


def csv_tentacle(path_to_file, id_column_number, missing_element=-1, custom_mapping: list=None):
    # more complex rules with dealing with missing_elements in the future? need user input for that
    """
    custom_mapping maps header strings into new header strings, list of tuples
    """
    pass


def folder_tentacle(path_to_folder, column_name: str, exclude_files_list: tuple=(), filename_transform=None,
                    keep_original_filename: bool=True, original_filename_column_name: str='filename', **kwargs):
    for (dirpath, dirnames, filenames) in walk(path_to_folder):
        for f_name in filenames:
            if filename_transform is None:
                if f_name not in exclude_files_list:
                    yield {column_name: f_name}
            else:
                if f_name not in exclude_files_list:
                    if keep_original_filename is True:
                        yield {original_filename_column_name: f_name, column_name: filename_transform(f_name, **kwargs)}
                    else:
                        yield {column_name: filename_transform(f_name, **kwargs)}


def dump_dataset(dataset, dump_path, dump_backend):
    pass


def copy_to_dataset(dataset1, dataset2):
    pass


def sync_datasets(dataset1, dataset2, abort_if_collision: bool=True):
    """
    Check if data in fields with same name is the same everywhere!!! If not, either rename or abort (specified
    by abort_if_collision) ds1[myfield] != ds2[myfield] -> ds1[myfield1], ds2[myfield2], copy around
    use dataset.rename_field for that
    return either none or a list of tuples of renamed fields: [(myfield1, myfield2)]
    """
    pass