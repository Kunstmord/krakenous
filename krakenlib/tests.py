__author__ = 'Viktor Evstratov, George Oblapenko'
import unittest
from krakenlib.dataset import DataSet
from krakenlib.prerolled import folder_tentacle, convert_multiple_features_numpy, convert_single_feature_numpy
import os.path
from os import makedirs
import numpy as np
import krakenlib.errors


def fake_extractor(add_data, add_metadata, val):
    return val


def a_very_fake_extractor(add_data, add_metadata):
    return 30239


def ext3(add_data, add_metadata):
    return 23930


def argfeature_test(add_data, add_metadata, i, j=2):
    return i + j


def numpy_feature(add_data, add_metadata):
    return np.zeros(3)


def numpy_feature2(add_data, add_metadata):
    return np.ones(2)


def string_feature(add_data, add_metadata):
    return 'test string feature'


class TestShelve(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset = DataSet('shelve', {'db_path': 'testshelve'})  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            if not os.path.isdir('testfolder'):
                makedirs('testfolder')
                for i in range(10):
                    open('testfolder/' + str(i), 'a').close()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')

    def test_extraction(self):
        self.dataset.extract_feature_full(fake_extractor, 23, overwrite_feature=True)
        assert self.dataset.feature_exists_global('fake_extractor') is True  # the feature exists

    def test_extraction_value(self):
        self.dataset.extract_feature_full(fake_extractor, 46, overwrite_feature=True)
        assert self.dataset.single_data_record(1, ('fake_extractor',))['fake_extractor'] == 46
        # the feature value is correct
        self.dataset.extract_feature_full(fake_extractor, 'meaning of life', overwrite_feature=True)
        assert self.dataset.single_data_record(1, ('fake_extractor',))['fake_extractor'] == 'meaning of life'
        # feature overwriting works correctly

    def test_insert_single_value(self):
        self.dataset.insert_single(5, 'extra_field', 'extra number', overwrite_existing=True)
        assert self.dataset.feature_exists(5, 'extra_field') is True  # inserts something
        assert self.dataset.feature_exists(4, 'extra_field') is False  # inserts only for the correct id
        self.assertRaises(krakenlib.errors.KrakenousException, self.dataset.insert_single,
                          5, 'extra_field', 3333)
        # ^ does not overwrite by accident and raies correct error
        self.dataset.insert_single(5, 'extra_field', 5555, overwrite_existing=True)
        assert self.dataset.single_data_record(5, ('extra_field',))['extra_field'] == 5555
        # overwrite works correctly

    def test_delete_feature(self):
        self.dataset.extract_feature_full(a_very_fake_extractor, overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True  # feature exists
        self.dataset.delete_feature('a_very_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False  # featue doesn't exist anymore

    def test_rename_feature(self):
        self.dataset.extract_feature_full(a_very_fake_extractor, overwrite_feature=True)
        self.dataset.extract_feature_full(ext3, overwrite_feature=True)
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
        self.dataset.extract_feature_full(a_very_fake_extractor, overwrite_feature=True)
        self.dataset.extract_feature_full(numpy_feature, overwrite_feature=True)
        self.dataset.extract_feature_full(string_feature, overwrite_feature=True)
        feature_names = self.dataset.feature_names()
        assert 'a_very_fake_extractor' in feature_names
        assert 'string_feature' in feature_names
        assert 'numpy_feature' in feature_names  # the features were returned

    def test_return_single_feature(self):
        feature = self.dataset.single_feature('string_feature')
        assert len(feature) == self.dataset.total_records  # default values work correctly, returns data for all records
        assert feature[0] == 'test string feature'  # returns the correct data value
        feature = self.dataset.single_feature('string_feature', start_id=3)
        assert len(feature) == self.dataset.total_records - 2
        feature = self.dataset.single_feature('string_feature', start_id=4, end_id=7)
        assert len(feature) == 4  # and the start_id, end_id parameters work correctly

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

    def test_feature_args(self):
        self.dataset.extract_feature_full(argfeature_test, 5,
                                          column_names=(), metadata_names=(), verbose=0, writeback=0,
                                          overwrite_feature=True, j=10)
        assert self.dataset.single_data_record(5, ('argfeature_test',))['argfeature_test'] == 15


class TestNumpyConvert(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset = DataSet('shelve', {'db_path': 'testshelve'})  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            if not os.path.isdir('testfolder'):
                makedirs('testfolder')
                for i in range(10):
                    open('testfolder/' + str(i), 'a').close()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')

    def test_return_single_feature_numpy(self):
        feature = convert_single_feature_numpy(self.dataset, 'numpy_feature')
        assert feature.shape == (self.dataset.total_records, 3,)
        assert feature[0, 1] == 0  # converts a single feature for each record into a single numpy array correctly

    def test_return_multiple_features_numpy(self):
        self.dataset.extract_feature_full(numpy_feature2, overwrite_feature=True)
        assert self.dataset.feature_exists_global('numpy_feature2') is True
        feature = convert_multiple_features_numpy(self.dataset, ('numpy_feature', 'numpy_feature2',))
        assert isinstance(feature, np.ndarray) == True
        assert feature.shape == (self.dataset.total_records, 5,)
        assert feature[0, 1] == 0
        assert feature[0, 4] == 1  # converts multiple features for each record into a single numpy array correctly


if __name__ == '__main__':
    unittest.main()