__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# and what about converters?
# if you want to return labels, write your own return_feature('label')


class DataSet(object):
    def __init__(self, path: str, backend: str):
        self.path = path
        self.backend = backend
        self.total_points = 0
        self.metadata = {}
        # add backend checking here! ('sqlite', 'mongodb', 'csv', 'pickle')

    def add_metadata(self, metadata: dict):
        self.metadata = metadata
        # for mdata in metadata:
        #     setattr(self, ...)

    def populate(self, tentacle, *args, **kwargs):
        """
        tentacle is a generator, returns a dictionary of stuff
        meta - global path? (for files in folder) -> THINK
        """
        for i in tentacle(*args, **kwargs):
            self.append_data_point(i)
        return None

    def append_data_point(self, data_point: dict):
        pass

    def extract_feature(self, extractor, *args, verbose: int=0, **kwargs):
        # what about SQL column types? if backend=sqlite -> extract for first data point ->
        # if list/numpy array/string -> store in string type, else (int/float/double) -> extract all, check - if all
        # ints, store in int, else in double
        # raise error if field with such name exists
        pass

    def extract_feature_dependent_feature(self, extractor, feature_names_and_types: list, *args, verbose: int=0,
                                          **kwargs):
        """
        feature_names_and_types - a list of tuples, each is (<feature_name>, <convert_numpy>)
        this is passed to return feature_single (searching by id), a list of tuples (<feature_name>, <feature>)
        is formed and passed to the extractor
        """
        #
        pass

    def delete_feature(self, feature_name: str):
        pass

    def rename_feature(self, original_feature_name: str, new_feature_name: str, overwrite_existing: bool=False):
        pass

    def force_write_feature(self, feature_name: str, feature: tuple):
        # if len(feature) != self.total_points:
        #     raise some error
        pass

    def return_feature(self, feature_name: str, convert_numpy: bool=False, start_id: int=0, end_id: int=-1):
        """
        add start_id / end_id
        end_id = -1 means return EVERYTHING in (start_id, end_id)
        """
        pass

    def return_feature_single(self, feature_name: str, search_for, search_by_field_name: str='id',
                              convert_numpy: bool=False):
        """
        return feature for a specific data point, which is the first data point for which the column
        specified by 'search_by_field_name' is equal to the 'search_for' parameter
        """
        pass

    def yield_datapoints(self, column_names: tuple):
        """
        yield a datapoint - dict? {'id': id, 'feat1':....}
        return only what is specified in column_names
        """
        pass