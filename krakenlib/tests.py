__author__ = 'Viktor Evstratov, George Oblapenko'
import unittest
from krakenlib.dataset import DataSet
from krakenlib.prerolled import folder_tentacle
import os.path
from os import makedirs
import numpy as np


def fake_extractor(add_data, add_metadata, val):
    return val


def a_very_fake_extractor(add_data, add_metadata):
    return 30239


def ext3(add_data, add_metadata):
    return 23930


def numpy_feature(add_data, add_metadata):
    return np.zeros(3)


def string_feature(add_data, add_metadata):
    return 'test string feature'


class TestShelve(unittest.TestCase):
    def setUp(self):
        """
        This creates a DataSet which uses the shelve backend. It also creates a folder with 10 files (named 0 to 9)
        to use with the folder_tentacle (if a testfolder already exists, this just reaches inside it and grabs what's
        there)
        """
        self.dataset = DataSet('shelve', {'shelve_path': 'testshelve'})  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            if not os.path.isdir('testfolder'):
                makedirs('testfolder')
                for i in range(10):
                    open('testfolder/' + str(i), 'a').close()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')

    def test_extraction(self):
        self.dataset.extract_feature(fake_extractor, 23, overwrite_feature=True)
        assert self.dataset.feature_exists_global('fake_extractor') is True  # the feature exists

    def test_extraction_value(self):
        self.dataset.extract_feature(fake_extractor, 46, overwrite_feature=True)
        assert self.dataset.return_single_data_record(1, ('fake_extractor',))['fake_extractor'] == 46
        # the feature value is correct
        self.dataset.extract_feature(fake_extractor, 'meaning of life', overwrite_feature=True)
        assert self.dataset.return_single_data_record(1, ('fake_extractor',))['fake_extractor'] == 'meaning of life'
        # feature overwriting works correctly

    def test_append_data_record(self):
        current_records = self.dataset.total_records
        self.dataset.append_data_record({'fake_extractor': 'NOT THE MEANING OF LIFE AT ALL'})
        assert self.dataset.total_records == current_records + 1  # the number of records changes
        assert self.dataset.return_single_data_record(current_records + 1, ('fake_extractor',))['fake_extractor']\
               == 'NOT THE MEANING OF LIFE AT ALL'  # the data in the record is correct
        assert self.dataset.return_single_data_record(2, ('fake_extractor',))['fake_extractor']\
               != 'NOT THE MEANING OF LIFE AT ALL'  # the data in other record doesn't change

    def test_insert_single_value(self):
        self.dataset.insert_single(5, 'extra_field', 'extra number')
        assert self.dataset.feature_exists(5, 'extra_field') is True  # inserts something
        assert self.dataset.feature_exists(4, 'extra_field') is False  # inserts only for the correct id
        self.dataset.insert_single(5, 'extra_field', 3333, overwrite_existing=False)
        assert self.dataset.return_single_data_record(5, ('extra_field',))['extra_field'] == 'extra_number'
        # ^ does not overwrite by accident
        self.dataset.insert_single(5, 'extra_field', 5555, overwrite_existing=True)
        assert self.dataset.return_single_data_record(5, ('extra_field',))['extra_field'] == 5555
        # overwrite works correctly

    def test_delete_feature(self):
        self.dataset.extract_feature(a_very_fake_extractor, overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True  # feature exists
        self.dataset.delete_feature('a_very_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False  # featue doesn't exist anymore

    def test_rename_feature(self):
        self.dataset.extract_feature(a_very_fake_extractor, overwrite_feature=True)
        self.dataset.extract_feature(ext3, overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True  # feature exists
        self.dataset.rename_feature('a_very_fake_extractor', 'renamed_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False  # original name doesn't exists
        assert self.dataset.feature_exists_global('renamed_fake_extractor') is True  # new name exists
        assert self.dataset.return_single_data_record(3, ('ext3',))['ext3'] == 23930  # original value is correct
        self.dataset.rename_feature('renamed_fake_extractor', 'ext3', overwrite_existing=True, writeback=3)
        assert self.dataset.feature_exists_global('renamed_fake_extractor') is False  # new name doesn't exist
        assert self.dataset.feature_exists_global('ext3') is True  # newest name still exists
        assert self.dataset.return_single_data_record(3, ('ext3',))['ext3'] == 30239  # and the value was overwritten

    def test_feature_names(self):
        self.dataset.extract_feature(a_very_fake_extractor, overwrite_feature=True)
        self.dataset.extract_feature(numpy_feature, overwrite_feature=True)
        self.dataset.extract_feature(string_feature, overwrite_feature=True)
        feature_names = self.dataset.return_feature_names()
        names = []
        lengths = []
        for i in feature_names:
            names.append(i[0])
            lengths.append(i[1])
        assert 'a_very_fake_extractor' in names
        assert 'string_feature' in names
        assert 'numpy_feature' in names  # the features were returned
        fake_index = names.index('a_very_fake_extractor')
        string_index = names.index('string_feature')
        numpy_index = names.index('numpy_feature')
        assert lengths[fake_index] == 0
        assert lengths[string_index] == 'string'
        assert lengths[numpy_index] == 3  # along with their correct lengths/types


if __name__ == '__main__':
    unittest.main()