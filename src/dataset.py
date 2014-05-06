__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# what about verbose extractors? separate?
# and what about converters?

class DataSet(object):
    def __init__(self, path: str, backend: str):
        self.path = path
        self.backend = backend
        self.total_points = 0
        # add backend checking here! ('sqlite', 'mongodb', 'csv')

    def append_data_point(self, data_point: dict):
        pass

    def extract_feature(self, extractor, *args, **kwargs):
        pass

    def extract_feature_dependent_feature(self, extractor, feature_names_and_types: list, *args, **kwargs):
        """
        feature_names_and_types - a list of tuples, each is (<feature_name>, <convert_numpy>)
        this is passed to return feature_single (searching by id), a list of tuples (<feature_name>, <feature>)
        is formed and passed to the extractor
        """
        #
        pass

    def delete_feature(self, feature_name: str):
        pass

    def rename_feature(self, original_feature_name: str, new_feature_name: str):
        pass

    def force_write_feature(self):
        pass

    def return_feature(self, feature_name: str, convert_numpy: bool=False):
        """
        add start_id / end_id
        """
        pass

    def return_feature_single(self, feature_name: str, search_for, search_by_field_name: str='id',
                              convert_numpy: bool=False):
        """
        return feature for a specific data point, which is the first data point for which the column
        specified by 'search_by_field_name' is equal to the 'search_for' parameter
        """
        pass

    def yield_datapoints(self, column_names: list):
        """
        yield a datapoint - dict? {'id': id, 'feat1':....}
        return only what is specified in column_names
        """
        pass

    def populate(self, tentacle, *args, **kwargs):
        """
        tentacle is a generator, returns a dictionary of stuff
        meta - global path? (for files in folder) -> THINK
        """
        for i in tentacle(*args, **kwargs):
            self.append_data_point(i)
        return None