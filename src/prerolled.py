__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"


def csv_tentacle(path_to_file, id_column_number, missing_element=-1):
    pass


def folder_tentacle(path_to_folder, exclude_files_list: list=None):
    pass


def dump_dataset(dataset, dump_path, dump_backend):
    pass


def copy_to_dataset(dataset1, dataset2):
    pass


def sync_datasets(dataset1, dataset2, abort_if_collision: bool=True):
    """
    Check if data in fields with same name is the same everywhere!!! If not, either rename or abort (specified
    by abort_if_collision)
    """
    pass