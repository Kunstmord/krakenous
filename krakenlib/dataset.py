__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# and what about converters?
# if you want to return labels, write your own return_feature('label')
from krakenlib.errors import *


class DataSet(object):
    def __init__(self, path: str, backend: str, metadata: dict=None):
        self.path = path
        self.backend = backend
        self.total_records = 0
        self.feature_consistency = {}
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata
        self.names = ()  # a tuple of all column names!
        # add backend checking here! ('sqlite', 'mongodb', 'csv', 'pickle')
        # load stuff from backend!

    def update_metadata(self, new_metadata: dict):
        """
        Keep this simple - overwrite any metadata with same key names
        """
        if self.metadata is {}:
            self.metadata = new_metadata
        else:
            self.metadata.update(new_metadata)
        return None
        # for mdata in metadata:
        #     setattr(self, ...)

    def get_all_metadata(self) -> dict:
        return self.metadata

    def get_metadata(self, keys: tuple) -> dict:
        result_dict = {}
        for key in keys:
            if key in self.metadata:
                result_dict[key] = self.metadata[key]
        return result_dict

    def populate(self, tentacle, *args, **kwargs):
        """
        tentacle is a generator, returns a dictionary of stuff
        meta - global path? (for files in folder) -> THINK
        """
        for i in tentacle(*args, **kwargs):
            self.append_data_record(i)
        return None

    def append_data_record(self, data_record: dict):
        """
        do not write id field! (if exists in dict)
        """
        pass

    def insert_single(self, id, feature_name, data, overwrite_existing: bool=False):
        """
        insert a single value (data) for a specific id
        """
        pass

    def extract_feature(self, extractor, *args, verbose: int=0, **kwargs):
        # what about SQL column types? if backend=sqlite -> extract for first data record ->
        # if list/numpy array/string -> store in string type, else (int/float/double) -> extract all, check - if all
        # ints, store in int, else in double
        # raise error if field with such name exists
        self.feature_consistency[extractor.__name__] = True
        return None

    def extract_feature_dependent_feature(self, extractor, feature_names_and_types: tuple, *args, verbose: int=0,
                                          **kwargs):
        """
        feature_names_and_types - a tuple of tuples, each is (<feature_name>, <convert_numpy>)
        this is passed to return feature_single (searching by id), a list of tuples (<feature_name>, <feature>)
        is formed and passed to the extractor
        """
        #
        self.feature_consistency[extractor.__name__] = True
        return None

    def mutate_feature(self, mutator, feature_names_and_types: tuple, *args, verbose: int=0, **kwargs):
        """
        do something with the data in a feature, and write it to the same column (basically, a wrapper for
        extract_feature_dependent_feature -> delete old feature -> rename extracted feature)
        sql -> what if the mutated feature is of a new type? string -> float?
        """
        pass

    def delete_feature(self, feature_name: str):
        if self.backend == 'sqlite':
            raise UnsupportedOperation('delete feature', 'sqlite')
        else:
            return 1

    def rename_feature(self, original_feature_name: str, new_feature_name: str, overwrite_existing: bool=False):
        pass

    def force_write_feature(self, feature_name: str, feature: list):
        # if len(feature) != self.total_records:
        #     raise some error
        pass

    def check_feature_existence(self, feature_name: str):
        return False

    def return_single_feature(self, feature_name: str, convert_numpy: bool=False, start_id: int=0, end_id: int=-1):
        """
        add start_id / end_id
        end_id = -1 means return EVERYTHING in (start_id, end_id)
        """
        pass

    def return_multiple_features(self, feature_names: tuple, convert_numpy: bool=False,
                                 start_id: int=0, end_id: int=-1):
        """
        if convert_numpy = false, returns list of tuples - (feature_name, feature)
        else returns a single enormous array
        add start_id / end_id
        end_id = -1 means return EVERYTHING in (start_id, end_id)
        """
        pass

    def return_feature_single_record(self, feature_name: str, search_for, search_by_field_name: str='id',
                                     convert_numpy: bool=False):
        """
        return feature for a specific data record, which is the first data record for which the column
        specified by 'search_by_field_name' is equal to the 'search_for' parameter
        """
        pass

    def return_feature_names(self):
        """
        return a list of tuples (<column_name>, <length>) - if number, length = 1, if list/np-array, length
        if string - what if it's a string? -1?
        """
        pass

    def get_single_data_record(self, record_id, column_names: tuple):
        """
        returns a dict
        """
        pass

    def yield_data_records(self, feature_names: tuple=()):
        """
        yield a datarecord - dict? {'id': id, 'feat1':....}
        return only what is specified in column_names; if () - return everything
        """
        for record_id in range(self.total_records):
            yield self.get_single_data_record(record_id, feature_names)

    def check_feature_consistency(self, feature_names: tuple=()):
        """
        check that features exist for each and every data record, if feature_names == () - run checks for every feature
        """
        #check, check, check
        return self.feature_consistency