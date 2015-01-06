__author__ = 'Viktor Evstratov, George Oblapenko'
import unittest
from krakenous.dataset import DataSet
from krakenous.prerolled import folder_tentacle, convert_multiple_features_numpy, convert_single_feature_numpy,\
    numpy_array_serializer, dump_dataset, numpy_array_deserializer, sync_datasets
import os.path
from os import makedirs, rmdir, remove
import numpy as np
import krakenous.errors


def fake_extractor(add_data, val):
    return val


def a_very_fake_extractor(add_data):
    return 30239


def ext3(add_data):
    return 23930


def list_feature_test(add_data):
    return [1, 3, 4]


def argfeature_test(add_data, i, j=2):
    return i + j


def numpy_feature(add_data):
    return np.zeros(3)


def numpy_feature2(add_data):
    return np.ones(2)


def numpy_feature_md(add_data):
    return np.ones([3, 4, 9])


def string_feature(add_data):
    return 'test string feature'


def id_doubler(add_data):
    return add_data['id'] * 2


def id_heavyside(add_data):
    if add_data['id'] > 5:
        return 1
    else:
        return 0


def folder_creator():
    if not os.path.isdir('testfolder'):
        makedirs('testfolder')
        for i in range(10):
            open('testfolder/' + str(i), 'a').close()


def folder_destroyer():
    for i in range(10):
        remove('testfolder/' + str(i))
    rmdir('testfolder')


class BaseTest(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset = DataSet(backend='shelve', db_path='testshelve')  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            folder_creator()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')

    def test_population(self):
        assert self.dataset.total_records == 10

    def test_extraction(self):
        self.dataset.extract_feature_full(fake_extractor, (), 23, overwrite_feature=True)
        assert self.dataset.feature_exists_global('fake_extractor') is True  # the feature exists

    def test_extraction_value(self):
        self.dataset.extract_feature_full(fake_extractor, (), 46, overwrite_feature=True)
        assert self.dataset.single_data_record(1, ('fake_extractor',))['fake_extractor'] == 46
        # the feature value is correct
        self.dataset.extract_feature_full(fake_extractor, (), 'meaning of life', overwrite_feature=True)
        assert self.dataset.single_data_record(1, ('fake_extractor',))['fake_extractor'] == 'meaning of life'
        # feature overwriting works correctly

    def test_insert_single_value(self):
        self.dataset.insert_single(5, 'extra_field', 'extra number', overwrite_existing=True)
        assert self.dataset.feature_exists(5, 'extra_field') is True  # inserts something
        assert self.dataset.feature_exists(4, 'extra_field') is False  # inserts only for the correct id
        self.assertRaises(krakenous.errors.KrakenousException, self.dataset.insert_single,
                          5, 'extra_field', 3333)
        # ^ does not overwrite by accident and raies correct error
        self.dataset.insert_single(5, 'extra_field', 5555, overwrite_existing=True)
        assert self.dataset.single_data_record(5, ('extra_field',))['extra_field'] == 5555
        # overwrite works correctly

    def test_feature_args(self):
        self.dataset.extract_feature_full(argfeature_test, (), 5,
                                          verbose=0, writeback=0,
                                          overwrite_feature=True, j=10)
        assert self.dataset.single_data_record(5, ('argfeature_test',))['argfeature_test'] == 15

    def test_return_single_feature(self):
        self.dataset.extract_feature_full(string_feature, (), overwrite_feature=True)
        feature = self.dataset.single_feature('string_feature')
        assert len(feature) == self.dataset.total_records  # default values work correctly, returns data for all records
        assert feature[0] == 'test string feature'  # returns the correct data value
        feature = self.dataset.single_feature('string_feature', start_id=3)
        assert len(feature) == self.dataset.total_records - 2
        feature = self.dataset.single_feature('string_feature', start_id=4, end_id=7)
        assert len(feature) == 4  # and the start_id, end_id parameters work correctly

    def test_column_selection(self):
        self.dataset.extract_feature_simple(id_doubler, ('id', ))
        feature = self.dataset.single_feature('id_doubler', start_id=1, end_id=3)
        assert feature[0] == 2
        assert feature[2] == 6

    def test_filter(self):
        tests = list(self.dataset.yield_data_records(('id',), filters={'id': 4}))
        assert len(tests) == 1
        tests = list(self.dataset.yield_data_records(('id',), filters={'id': 114}))
        assert len(tests) == 0
        self.dataset.extract_feature_simple(id_heavyside, ('id',))
        tests = list(self.dataset.yield_data_records(('id_heavyside',), filters={'id_heavyside': 0}))
        assert len(tests) == 5

    def tearDown(self):
        folder_destroyer()
        remove('testshelve.db')


class TestShelve(BaseTest):
    def test_delete_feature(self):
        self.dataset.extract_feature_full(a_very_fake_extractor, (), overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True  # feature exists
        self.dataset.delete_feature('a_very_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False  # featue doesn't exist anymore

    def test_rename_feature(self):
        self.dataset.extract_feature_full(a_very_fake_extractor, (), overwrite_feature=True)
        self.dataset.extract_feature_full(ext3, (), overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True  # feature exists
        self.dataset.rename_feature('a_very_fake_extractor', 'renamed_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False  # original name doesn't exists
        assert self.dataset.feature_exists_global('renamed_fake_extractor') is True  # new name exists
        assert self.dataset.single_data_record(3, ('ext3',))['ext3'] == 23930  # original value is correct
        self.dataset.rename_feature('renamed_fake_extractor', 'ext3', overwrite_existing=True, writeback=3)
        assert self.dataset.feature_exists_global('renamed_fake_extractor') is False  # new name doesn't exist
        assert self.dataset.feature_exists_global('ext3') is True  # newest name still exists
        assert self.dataset.single_data_record(3, ('ext3',))['ext3'] == 30239  # and the value was overwritten

    def test_feature_names(self):
        self.dataset.extract_feature_full(a_very_fake_extractor, (), overwrite_feature=True)
        self.dataset.extract_feature_full(numpy_feature, (), overwrite_feature=True)
        self.dataset.extract_feature_full(string_feature, (), overwrite_feature=True)
        feature_names = self.dataset.feature_names()
        assert 'a_very_fake_extractor' in feature_names
        assert 'string_feature' in feature_names
        assert 'numpy_feature' in feature_names  # the features were returned

    def test_append_and_delete_data_record(self):
        current_records = self.dataset.total_records
        self.dataset.append_data_record({'fake_extractor': 'NOT THE MEANING OF LIFE AT ALL'})
        assert self.dataset.total_records == current_records + 1  # the number of records changes
        assert self.dataset.single_data_record(current_records + 1, ('fake_extractor',))['fake_extractor']\
               == 'NOT THE MEANING OF LIFE AT ALL'  # the data in the record is correct
        assert self.dataset.single_data_record(2, ('fake_extractor',))['fake_extractor']\
               != 'NOT THE MEANING OF LIFE AT ALL'  # the data in other record doesn't change
        self.dataset.delete_records((self.dataset.total_records,))
        assert self.dataset.total_records == current_records  # the number of records changes back to the original
        assert self.dataset.single_data_record(current_records, ('fake_extractor',))['fake_extractor']\
               != 'NOT THE MEANING OF LIFE AT ALL'  # the last record is correct


class TestSQLite(BaseTest):
    def setUp(self):
        self.dataset = DataSet(backend='sqlite', db_path='testsqlite', table_name='test_table')
        if self.dataset.total_records == 0:
            folder_creator()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')

    def test_deserialize(self):
        self.dataset.extract_feature_simple_custom_serializer(numpy_feature_md, (), numpy_array_serializer)
        np_feat = self.dataset.single_data_record(5, ('numpy_feature_md', ),
                                                  {'numpy_feature_md': numpy_array_deserializer})
        assert np_feat['numpy_feature_md'].shape == (3, 4, 9)

    def test_feature_names_and_serialization(self):
        self.dataset.extract_feature_full(a_very_fake_extractor, (), overwrite_feature=True)
        self.dataset.extract_feature_full(list_feature_test, (), overwrite_feature=True)
        self.dataset.extract_feature_full(numpy_feature, (), overwrite_feature=True, serializer=numpy_array_serializer)
        self.dataset.extract_feature_full(string_feature, (), overwrite_feature=True)
        feature_names = self.dataset.feature_names()
        assert 'a_very_fake_extractor' in feature_names
        assert 'string_feature' in feature_names
        assert 'numpy_feature' in feature_names  # the features were returned

    def tearDown(self):
        folder_destroyer()
        remove('testsqlite')


class TestNumpyConvert(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset = DataSet(backend='shelve', db_path='testshelve')  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            folder_creator()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')
        self.dataset.extract_feature_simple(numpy_feature, ())
        self.dataset.extract_feature_simple(numpy_feature2, ())

    def test_return_single_feature_numpy(self):
        feature = convert_single_feature_numpy(self.dataset, 'numpy_feature')
        assert feature.shape == (self.dataset.total_records, 3,)
        assert feature[0, 1] == 0  # converts a single feature for each record into a single numpy array correctly

    def test_return_multiple_features_numpy(self):
        self.dataset.extract_feature_full(numpy_feature2, (), overwrite_feature=True)
        assert self.dataset.feature_exists_global('numpy_feature2') is True
        feature = convert_multiple_features_numpy(self.dataset, ('numpy_feature', 'numpy_feature2',))
        assert isinstance(feature, np.ndarray) == True
        assert feature.shape == (self.dataset.total_records, 5,)
        assert feature[0, 1] == 0
        assert feature[0, 4] == 1  # converts multiple features for each record into a single numpy array correctly

    def tearDown(self):
        folder_destroyer()
        remove('testshelve.db')


class TestDump(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset = DataSet(backend='shelve', db_path='testshelve')  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            folder_creator()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')
        self.dataset.extract_feature_simple(ext3, ())
        self.dataset.extract_feature_simple(string_feature, ())
        self.dataset.extract_feature_simple(numpy_feature, ())

    def test_full_dump(self):
        new_dataset = DataSet(backend='sqlite', db_path='testsqlite1', table_name='test_table')
        dump_dataset(self.dataset, new_dataset, serializers={'numpy_feature': numpy_array_serializer})
        assert new_dataset.total_records == self.dataset.total_records
        feature_names = new_dataset.feature_names()
        assert 'string_feature' in feature_names
        assert 'ext3' in feature_names
        assert 'numpy_feature' in feature_names

    def tearDown(self):
        folder_destroyer()
        remove('testshelve.db')
        remove('testsqlite1')


class TestSync(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset1 = DataSet(backend='sqlite', db_path='testsqlite1', table_name='test_table')
        self.dataset2 = DataSet(backend='sqlite', db_path='testsqlite2', table_name='test_table')
        if self.dataset1.total_records == 0:
            folder_creator()
            self.dataset1.populate(folder_tentacle, 'testfolder', 'filename')
            self.dataset2.populate(folder_tentacle, 'testfolder', 'filename')
        self.dataset1.extract_feature_simple(ext3, ())
        self.dataset1.extract_feature_simple(string_feature, ())
        self.dataset2.extract_feature_simple_custom_serializer(numpy_feature, (), numpy_array_serializer)

    def test_syncing(self):
        sync_datasets(self.dataset1, self.dataset2, serializers={'numpy_feature': numpy_array_serializer},
                      deserializers={'numpy_feature': numpy_array_deserializer})
        names1 = self.dataset1.feature_names()
        names2 = self.dataset2.feature_names()
        assert 'numpy_feature' in names1
        assert 'ext3' in names2
        assert 'string_feature' in names2

    def tearDown(self):
        folder_destroyer()
        remove('testsqlite1')
        remove('testsqlite2')


if __name__ == '__main__':
    unittest.main()