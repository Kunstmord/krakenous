__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
# if you want to return labels, write your own return_feature('label')
# _safe versions (which do not check for fields named 'id' or other fields that are protected? Store protected
# fields in self.metadata?) And check otherwise?
from krakenlib.errors import UnsupportedOperation, UnknownBackend, DoesNotExist, DataPropertyNameCollision, \
    ProtectedField, KrakenlibException
import os.path


class DataSet(object):
    def __init__(self, backend_name: str, db_data: dict, metadata: dict=None):
        if backend_name not in ('sqlite', 'shelve'):
            raise UnknownBackend('Unknown backend ' + backend_name)
        elif backend_name == 'shelve':
            global backend
            import krakenlib.backend_shelve as backend
            # if not os.path.isfile(db_data['shelve_path']):  # nothing exists
            #     backend.create_empty(db_data)

            # counting of records?
        self.db_data = db_data
        self.backend_name = backend_name
        self.total_records = 0
        # self.feature_consistency = {}
        if metadata is None:
            self.metadata = {}
        else:
            self.metadata = metadata

    def update_metadata(self, new_metadata: dict):
        """
        FINISHED
        """
        if self.metadata is {}:
            self.metadata = new_metadata
        else:
            self.metadata.update(new_metadata)
        return None

    def populate(self, tentacle, *args, writeback: int=0, **kwargs) -> int:
        """
        tentacle is a generator, returns a dictionary of stuff
        """
        records_added = 0
        if self.backend_name == 'shelve':
            if writeback != 0:
                db = backend.open_db(self.db_data, True)
            else:
                db = backend.open_db(self.db_data, False)
        for i in tentacle(*args, **kwargs):
            self._append_data_record_open(i, db)
            records_added += 1
            if writeback != 0 and self.total_records % writeback == 0:
                backend.commit_db(db)
        self.total_records = records_added
        backend.close_db(db)
        return records_added

    def append_data_record(self, data_record: dict):
        """
        do not write id field! (if exists in dict)
        """
        # if db is None:
        if self.backend_name == 'shelve':
            db = backend.open_db(self.db_data)
        self.total_records += 1

        for data_entry in data_record:
            backend.write_data(db, self.total_records, data_entry, data_record[data_entry])
        backend.close_db(db)

    def _append_data_record_open(self, data_record: dict, db):
        """
        do not write id field! (if exists in dict)
        """
        self.total_records += 1

        for data_entry in data_record:
            backend.write_data(db, self.total_records, data_entry, data_record[data_entry])

    def insert_single(self, record_id: int, feature_name: str, data, overwrite_existing: bool=False):
        """
        insert a single value (data) for a specific id
        """
        if self.backend_name == 'shelve':
            if overwrite_existing is True:
                db = backend.open_db(self.db_data, True)
            else:
                db = backend.open_db(self.db_data, False)
        backend.write_data(db, record_id, feature_name, data)
        backend.close_db(db)

    def extract_feature(self, extractor, column_names: tuple, metadata_names: tuple, *args, verbose: int=0,
                        writeback: int=0, **kwargs):
        # what about SQL column types? if backend=sqlite -> extract for first data record ->
        # self.feature_consistency[extractor.__name__] = True
        extractor_name = extractor.__name__
        if self.check_global_feature_existence(extractor_name) is True:
            raise DataPropertyNameCollision('Feature with name "' + extractor_name + '" already exists')
        if extractor_name == 'id':
            raise ProtectedField('Cannot write to field "id"')
        if self.backend_name == 'shelve':
            if writeback != 0:
                db = backend.open_db(self.db_data, True)
            else:
                db = backend.open_db(self.db_data, False)

        for record_id in range(self.total_records):
            additional_data = {}
            additional_metadata = {}
            for column_name in column_names:
                additional_data[column_name] = db.read_data(db, record_id + 1, column_name)
            for metadata_name in metadata_names:
                additional_metadata[metadata_name] = self.metadata[metadata_name]
            backend.write_data(db, record_id + 1, extractor_name, extractor(additional_data, additional_metadata,
                                                                            *args, **kwargs))
            if verbose != 0 and self.total_records % verbose == 0:
                print(record_id + 1)
            if writeback != 0 and self.total_records % writeback == 0:
                backend.commit_db(db)
        backend.close_db(db)

    def mutate_feature(self, mutator, feature_names_and_types: tuple, *args, verbose: int=0, **kwargs):
        """
        do something with the data in a feature, and write it to the same column (basically, a wrapper for
        extract_feature_dependent_feature -> delete old feature -> rename extracted feature)
        sql -> what if the mutated feature is of a new type? string -> float?
        """
        pass

    def delete_feature(self, feature_name: str):
        if self.backend_name == 'sqlite':
            raise UnsupportedOperation('Cannot delete feature when using a sqlite backend')
        else:
            return 1

    def rename_feature(self, original_feature_name: str, new_feature_name: str, overwrite_existing: bool=False):
        pass

    def force_write_feature(self, feature_name: str, feature: list, writeback: int=0):
        if len(feature) != self.total_records:
            raise KrakenlibException('Inconsistent length (length of feature list is ' + str(len(feature)) +
                                     ', total number of records - ' + str(self.total_records))
        elif feature_name == 'id':
            raise ProtectedField('Cannot write to field "id"')
        else:
            if writeback != 0:
                db = backend.open_db(self.db_data, True)
            else:
                db = backend.open_db(self.db_data, False)
            for record_id in range(self.total_records):
                backend.write_data(db, record_id + 1, feature_name, feature[record_id])
                if writeback != 0 and self.total_records % writeback == 0:
                    backend.commit_db(db)
            backend.close_db(db)

    def check_feature_existence(self, record_id: int, feature_name: str) -> bool:
        if self.backend_name == 'shelve':
            db = backend.open_db(self.db_data, False)
        feature_exists = backend.feature_exists(db, record_id, feature_name)
        backend.close_db(db)
        return feature_exists


    def _check_feature_existence_open(self, record_id: int, feature_name: str, db) -> bool:
        feature_exists = backend.feature_exists(db, record_id, feature_name)
        return feature_exists


    def check_global_feature_existence(self, feature_name: str) -> bool:
        if self.backend_name == 'shelve':
            db = backend.open_db(self.db_data, False)
        for record_id in range(self.total_records):
            if self._check_feature_existence_open(record_id + 1, feature_name, db) is True:
                backend.close_db(db)
                return True
        backend.close_db(db)
        return False

    def return_single_feature(self, feature_name: str, convert_numpy: bool=False, start_id: int=0, end_id: int=-1):

        """
        add start_id / end_id
        end_id = -1 means return EVERYTHING in (start_id, end_id)
        """
        pass

    def return_multiple_features(self, feature_names: tuple, convert_numpy: bool=False,
                                 start_id: int=0, end_id: int=-1) -> list:
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

    def return_feature_names(self) -> list:
        """
        return a list of tuples (<column_name>, <length>) - if number, length = 1, if list/np-array, length
        if string - what if it's a string? -1?
        assume that the first element has all the features? or combine all? or return all + all separate variations?
        """
        pass

    def return_single_data_record(self, record_id, column_names: tuple=()) -> dict:
        """
        returns a dict
        """
        result_dict = {}
        if self.backend_name == 'shelve':
            db = backend.open_db(self.db_data)
        if column_names == ():
            return backend.read_all_data(db, record_id)
        else:
            for column_name in column_names:
                result_dict[column_name] = backend.read_data(record_id, column_name)
            return result_dict

    def yield_data_records(self, column_names: tuple=()) -> dict:
        """
        FINISHED
        yield a datarecord - dict? {'id': id, 'feat1':....}
        return only what is specified in column_names; if () - return everything
        """
        for record_id in range(self.total_records):
            yield self.return_single_data_record(record_id, column_names)

    def return_id_filter_by_feature(self, feature_name: str, filter_function, *args, **kwargs) -> list:
        """
        Need to check consistency??
        Returns a list of ids of data records for which the feature specified by feature_name satisfies
        a condition set by filter_function(feature_name, *args, **kwargs) (the filter_function should
        return True if the condition is satisfied, False otherwise)
        """
        if self.check_global_feature_existence(feature_name) is False:
            raise DoesNotExist('Feature "' + feature_name + '" does not exist')
        else:
            result = []
            for data_record in self.yield_data_records(('id', feature_name,)):
                if filter_function(data_record[feature_name], *args, **kwargs) is True:
                    result.append(data_record['id'])
            return result

    # def check_feature_consistency(self, feature_names: tuple=()) -> dict:
    #     """
    #     check that features exist for each and every data record, if feature_names == () - run checks for every feature
    #     """
    #     #check, check, check
    #     return self.feature_consistency