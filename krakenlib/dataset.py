__author__ = 'George Oblapenko, Viktor Evstratov'
__license__ = "GPLv3"
from krakenlib.errors import *
import os.path
from json import dumps


class DataSet(object):
    def __init__(self, **kwargs):
        self.total_records = 0

        if 'backend' not in kwargs:
            raise KrakenousException('No backend specified')
        else:
            self.backend_name = kwargs['backend']

        if self.backend_name == 'shelve':
            if 'db_path' not in kwargs:
                raise KrakenousException('No path to shelve file specified')
            else:
                self.db_data = {'db_path': kwargs['db_path']}
                self.backend = __import__('krakenlib.backend_shelve', fromlist=['backend_shelve'])
                if os.path.isfile(self.db_data['db_path'] + '.db'):
                    self.total_records = self.backend.data_records_amount(self.db_data)
        elif self.backend_name == 'sqlite':
            if 'db_path' not in kwargs:
                raise KrakenousException('No path to SQLite file specified')
            elif 'table_name' not in kwargs:
                raise KrakenousException('No SQLite table name specified')
            else:
                self.db_data = {'db_path': kwargs['db_path'], 'table_name': kwargs['table_name']}
                self.backend = __import__('krakenlib.backend_sqlite', fromlist=['backend_sqlite'])
                if os.path.isfile(self.db_data['db_path']):
                    self.total_records = self.backend.data_records_amount(self.db_data)
                else:
                    self.backend.create_db(self.db_data)
        else:
            raise KrakenousException('Unknown backend ' + self.backend_name)

    def get_end_id(self, start_id, end_id):
        if self.total_records == 0:
            raise KrakenousException('The dataset is empty!')
        if end_id == -1:
            end_id = self.total_records
        else:
            if end_id > self.total_records:
                raise KrakenousException('Incorrect range, end_id=' + str(end_id), ', while total_records='
                                         + str(self.total_records))
        if start_id < 0 or start_id > end_id:
            raise KrakenousException('start_id cannot be less than 0 or bigger than end_id')
        return end_id

    def populate(self, tentacle, *args, writeback: int=0, **kwargs) -> int:
        #TODO custom serializer support
        """
        tentacle is a generator, returns a dictionary of stuff
        """
        records_added = 0
        if self.backend_name in ('shelve', 'sqlite',):
            if writeback != 0:
                db = self.backend.open_db(self.db_data, True)
            else:
                db = self.backend.open_db(self.db_data, False)
        for tentacle_dict in tentacle(*args, **kwargs):
            if self.backend_name == 'sqlite':
                try:
                    all_names = self.feature_names()
                except KrakenousException:
                    all_names = []
                for tentacle_key in tentacle_dict:
                    if tentacle_key not in all_names:
                        self.backend.create_new_column(db, tentacle_key)
            self._append_data_record_open(tentacle_dict, db)
            records_added += 1
            if writeback != 0 and self.total_records % writeback == 0:
                self.backend.commit_db(db)
        self.total_records = records_added
        self.backend.close_db(db)

    def append_data_record(self, data_record: dict, serializers=None):
        """
        do not write id field! (if exists in dict) (doesn't concern shelve)
        """
        if self.backend_name in ('shelve', 'sqlite',):
            db = self.backend.open_db(self.db_data)
        if self.backend_name == 'shelve':
            for data_entry in data_record:
                self.backend.write_data(db, self.total_records + 1, data_entry, data_record[data_entry])
        elif self.backend_name == 'sqlite':
            if 'id' in data_record.keys():
                raise KrakenousException('Cannot write to id column')
            try:
                all_names = self.feature_names()
            except KrakenousException:
                all_names = []
            for data_key in data_record:
                if data_key not in all_names:
                    self.backend.create_new_column(db, data_key)
            self.backend.append_data_record(db, 1, data_record, serializers)
        self.total_records += 1
        self.backend.close_db(db)

    def _append_data_record_open(self, data_record: dict, db):
        """
        do not write id field! (if exists in dict) (doesn't concern shelve)
        """
        self.total_records += 1

        self.backend.append_data_record(db, self.total_records, data_record)
        # for data_entry in data_record:
        #     backend.write_data(db, self.total_records, data_entry, data_record[data_entry])

    def insert_single(self, record_id: int, feature_name: str, data, overwrite_existing: bool=False, serializer=None):
        """
        insert a single value (data) for a specific id
        """
        if not overwrite_existing and self.feature_exists(record_id, feature_name):
            raise KrakenousException('Feature with name "' + feature_name + '" already exists')
        db = self.backend.open_db(self.db_data)
        if self.backend_name == 'sqlite':
            if not serializer:
                serializer = dumps
            if feature_name not in self.feature_names():
                self.backend.create_new_column(db, feature_name)
        if self.backend_name == 'sqlite' and feature_name not in self.feature_names():
            self.backend.create_new_column(db, feature_name)
        if self.backend_name == 'shelve':
            self.backend.write_data(db, record_id, feature_name, data)
        else:
            self.backend.write_data(db, record_id, feature_name, data, serializer)
        self.backend.close_db(db)

    def extract_feature_full_for_range(self, start_id: int, end_id: int,
                                       extractor, column_names, *args,
                                       verbose: int=0, writeback: int=0, overwrite_feature: bool=False,
                                       serializer=None, **kwargs):
        end_id = self.get_end_id(start_id, end_id)
        extractor_name = extractor.__name__
        global_existence = self.feature_exists_global(extractor_name)
        if not overwrite_feature and global_existence:
            raise KrakenousException('Feature with name "' + extractor_name + '" already exists')
        if extractor_name == 'id':
            raise KrakenousException('Cannot write to field "id"')
        if writeback != 0:
            db = self.backend.open_db(self.db_data, True)
        else:
            db = self.backend.open_db(self.db_data, False)

        if self.backend_name == 'sqlite':
            if not global_existence:
                self.backend.create_new_column(db, extractor_name)
            if not serializer:
                serializer = dumps

        for record_id in range(start_id, end_id + 1):
            additional_data = {}
            for column_name in column_names:
                if column_name != 'id':
                    additional_data[column_name] = self.backend.read_single_data(db, record_id, column_name)
                else:
                    additional_data[column_name] = record_id
            if self.backend_name == 'shelve':
                self.backend.write_data(db, record_id, extractor_name, extractor(additional_data,
                                                                                 *args, **kwargs))
            elif self.backend_name == 'sqlite':
                self.backend.write_data(db, record_id, extractor_name, extractor(additional_data,
                                                                                 *args, **kwargs), serializer)

            if verbose != 0 and record_id % verbose == 0:
                print(record_id)
            if writeback != 0 and record_id % writeback == 0:
                self.backend.commit_db(db)
        self.backend.close_db(db)

    def extract_feature_full(self, extractor, column_names, *args, verbose: int=0,
                             writeback: int=0, overwrite_feature: bool=False, serializer=None, **kwargs):
        self.extract_feature_full_for_range(1, -1, extractor, column_names, *args, verbose=verbose,
                                            writeback=writeback, overwrite_feature=overwrite_feature,
                                            serializer=serializer, **kwargs)

    def extract_feature_simple(self, extractor, column_names, *args, **kwargs):
        self.extract_feature_full_for_range(1, -1, extractor, column_names, *args, verbose=0,
                                            writeback=0, overwrite_feature=False, serializer=None, **kwargs)

    def extract_feature_simple_custom_serializer(self, extractor, column_names, serializer, *args, **kwargs):
        self.extract_feature_full_for_range(1, -1, extractor, column_names, *args, verbose=0,
                                            writeback=0, overwrite_feature=False, serializer=serializer, **kwargs)

    def extract_dependent_feature_simple(self, extractor, column_names, *args, **kwargs):
        self.extract_feature_full_for_range(1, -1, extractor, column_names, *args, verbose=0,
                                            writeback=0, overwrite_feature=False, serializer=None, **kwargs)

    def extract_dependent_feature_simple_custom_serializer(self, extractor, column_names, serializer, *args, **kwargs):
        self.extract_feature_full_for_range(1, -1, extractor, column_names, *args, verbose=0,
                                            writeback=0, overwrite_feature=False, serializer=serializer, **kwargs)

    def delete_feature(self, feature_name: str, writeback: int=0):
        """
        delete the whole column of features
        :param feature_name:
        :param writeback:
        :return:
        """
        if self.backend_name == 'sqlite':
            raise KrakenousUnsupportedOperation('Deletion of features is not available for the SQLite backend')
        if self.total_records == 0:
            raise KrakenousException('The dataset is empty!')
        else:
            if writeback != 0:
                db = self.backend.open_db(self.db_data, True)
            else:
                db = self.backend.open_db(self.db_data, False)
            for record_id in range(self.total_records):
                self.backend.delete_data(db, record_id + 1, feature_name)
                if writeback != 0 and record_id % writeback == 0:
                    self.backend.commit_db(db)
            self.backend.close_db(db)

    def rename_feature(self, original_feature_name: str, new_feature_name: str, overwrite_existing: bool=False,
                       writeback: int=0):
        if self.backend_name == 'sqlite':
            raise KrakenousUnsupportedOperation('Renaming of features is not available for the SQLite backend')
        if self.total_records == 0:
            raise KrakenousException('The dataset is empty!')
        if not overwrite_existing and self.feature_exists_global(new_feature_name):
            raise KrakenousException('Feature with name "' + new_feature_name + '" already exists')

        if self.backend_name == 'shelve':
            if writeback != 0:
                db = self.backend.open_db(self.db_data, True)
            else:
                db = self.backend.open_db(self.db_data, False)
            for record_id in range(self.total_records):
                self.backend.write_data(db, record_id + 1, new_feature_name, self.backend.read_single_data(db, record_id + 1,
                                                                                          original_feature_name))
                self.backend.delete_data(db, record_id + 1, original_feature_name)
                if writeback != 0 and record_id % writeback == 0:
                    self.backend.commit_db(db)
            self.backend.close_db(db)

    def delete_records(self, record_ids: tuple, writeback: int=0):
        if self.backend_name == 'shelve':
            if writeback != 0:
                db = self.backend.open_db(self.db_data, True)
            else:
                db = self.backend.open_db(self.db_data, False)
        elif self.backend_name == 'sqlite':
            raise KrakenousNotImplemented('Deleting records is not implemented for the SQLite backend')
        if self.total_records == 0:
            raise KrakenousException('The dataset is empty!')
        for record_id in record_ids:
            if record_id > self.total_records:
                raise KrakenousException('Record with id=' + str(record_id) + ' does not exist!')
            else:
                self.backend.delete_record(db, record_id, self.total_records)
                if writeback != 0 and record_id % writeback == 0:
                    self.backend.commit_db(db)
                self.total_records -= 1

    def force_write_feature(self, feature_name: str, feature: list, writeback: int=0):
        if len(feature) != self.total_records:
            raise KrakenousException('Inconsistent length (length of feature list is ' + str(len(feature)) +
                                     ', total number of records - ' + str(self.total_records))
        elif feature_name == 'id':
            raise KrakenousException('Cannot write to field "id"')
        else:
            if writeback != 0:
                db = self.backend.open_db(self.db_data, True)
            else:
                db = self.backend.open_db(self.db_data, False)
            for record_id in range(self.total_records):
                self.backend.write_data(db, record_id + 1, feature_name, feature[record_id])
                if writeback != 0 and self.total_records % writeback == 0:
                    self.backend.commit_db(db)
            self.backend.close_db(db)

    def feature_exists(self, record_id: int, feature_name: str) -> bool:
        db = self.backend.open_db(self.db_data)
        if self.backend_name == 'sqlite' and feature_name not in self.feature_names():
            self.backend.close_db(db)
            return False
        else:
            feature_exists = self.backend.data_exists(db, record_id, feature_name)
            self.backend.close_db(db)
            return feature_exists

    def _feature_exists_open(self, record_id: int, feature_name: str, db) -> bool:
        feature_exists = self.backend.data_exists(db, record_id, feature_name)
        return feature_exists

    def feature_exists_global(self, feature_name: str) -> bool:
        if feature_name in self.feature_names():
            return True
        else:
            return False

    def single_feature(self, feature_name: str, start_id: int=1, end_id: int=-1, deserializer=None):
        end_id = self.get_end_id(start_id, end_id)
        if not self.feature_exists_global(feature_name):
            raise KrakenousException('Feature "' + feature_name + '" does not exist')
        db = self.backend.open_db(self.db_data)
        result = []
        if self.backend_name == 'sqlite':
            for record_id in range(start_id, end_id + 1):
                result.append(self.backend.read_single_data(db, record_id, feature_name, deserializer))
        else:
            for record_id in range(start_id, end_id + 1):
                result.append(self.backend.read_single_data(db, record_id, feature_name))
        self.backend.close_db(db)
        return result

    def multiple_features(self, feature_names: tuple, start_id: int=1, end_id: int=-1, deserializers: dict=None) -> list:
        """
        returns list of dictionaries
        add start_id / end_id
        end_id = -1 means return EVERYTHING in (start_id, end_id)
        """
        end_id = self.get_end_id(start_id, end_id)
        for feature_name in feature_names:
            if not self.feature_exists_global(feature_name):
                raise KrakenousException('Feature "' + feature_name + '" does not exist')
        db = self.backend.open_db(self.db_data)
        result = []
        if self.backend_name == 'sqlite':
            for record_id in range(start_id, end_id + 1):
                result.append(self.backend.read_multiple_data(db, record_id, feature_names, deserializers))
        else:
            for record_id in range(start_id, end_id + 1):
                result.append(self.backend.read_multiple_data(db, record_id, feature_names))
        self.backend.close_db(db)
        return result

    def feature_names(self, record_id=-1) -> list:
        if self.total_records == 0:
            raise KrakenousException('The dataset is empty!')
        db = self.backend.open_db(self.db_data)
        return_list = []
        if self.backend_name == 'shelve':
            if record_id == -1:
                for r_id in range(self.total_records):
                    tmp_list = self.backend.all_data_names(db, r_id + 1)
                    for tmp_f in tmp_list:
                        if tmp_f not in return_list:
                            return_list.append(tmp_f)
            else:
                return_list = self.backend.all_data_names(db, record_id)
        elif self.backend_name == 'sqlite':
            return_list = self.backend.all_data_names(db)
        self.backend.close_db(db)
        return return_list

    def _single_data_record_open(self, record_id, db, column_names: tuple=(), deserializers: dict=None) -> dict:
        """
        returns a dict
        """
        result_dict = {}
        if column_names == ():
            result_dict = self.backend.read_all_data(db, record_id)
            result_dict['id'] = record_id
        else:
            for column_name in column_names:
                if column_name != 'id':
                    if self.backend_name == 'sqlite' and deserializers and column_name in deserializers:
                        result_dict[column_name] = self.backend.read_single_data(db, record_id, column_name,
                                                                                 deserializers[column_name])
                    else:
                        result_dict[column_name] = self.backend.read_single_data(db, record_id, column_name)
                else:
                    result_dict['id'] = record_id
        return result_dict

    def single_data_record(self, record_id, column_names: tuple=(), deserializers=None) -> dict:
        """
        returns a dict
        """
        result_dict = {}
        db = self.backend.open_db(self.db_data)
        if column_names == ():
            if self.backend_name == 'shelve':
                result_dict = self.backend.read_all_data(db, record_id)
            elif self.backend_name == 'sqlite':
                tmp_res = self.backend.read_all_data(db, record_id, deserializers)
                all_column_names = self.backend.all_data_names(db)
                for column_name_enum in enumerate(all_column_names):
                    result_dict[column_name_enum[1]] = tmp_res[column_name_enum[0]]
            result_dict['id'] = record_id
        else:
            if 'id' in column_names:
                result_dict['id'] = record_id
            if self.backend_name == 'shelve':
                for column_name in column_names:
                    result_dict[column_name] = self.backend.read_single_data(db, record_id, column_name)
            elif self.backend_name == 'sqlite':
                for column_name in column_names:
                    if column_name != 'id':
                        if deserializers and column_name in deserializers:
                            result_dict[column_name] = self.backend.read_single_data(db, record_id, column_name,
                                                                                     deserializers[column_name])
                        else:
                            result_dict[column_name] = self.backend.read_single_data(db, record_id, column_name)
        self.backend.close_db(db)
        return result_dict

    def yield_data_records(self, column_names: tuple=(), start_id: int=1, end_id: int=-1, deserializers: dict=None,
                           filters=None) -> dict:
        """
        return only what is specified in column_names; if () - return everything
        """
        db = self.backend.open_db(self.db_data)
        end_id = self.get_end_id(start_id, end_id)
        for record_id in range(start_id, end_id + 1):
            if filters:
                data_record = self._single_data_record_open(record_id, db, column_names, deserializers)
                flag = True
                for column_name in filters:
                    if data_record[column_name] != filters[column_name]:
                        flag = False
                if flag:
                    yield data_record
            else:
                yield self._single_data_record_open(record_id, db, column_names, deserializers)
        self.backend.close_db(db)