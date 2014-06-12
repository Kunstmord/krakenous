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


class TestShelve(unittest.TestCase):
    def setUp(self):
        self.dataset = DataSet('shelve', {'shelve_path': 'testshelve'})  # we will use the folder tentacle for testing
        if self.dataset.total_records == 0:
            if not os.path.isdir('testfolder'):
                makedirs('testfolder')
                for i in range(10):
                    open('testfolder/' + str(i), 'a').close()
            self.dataset.populate(folder_tentacle, 'testfolder', 'filename')

    def test_extraction(self):
        self.dataset.extract_feature(fake_extractor, 23, overwrite_feature=True)
        assert self.dataset.feature_exists_global('fake_extractor') is True

    def test_extraction_value(self):
        self.dataset.extract_feature(fake_extractor, 46, overwrite_feature=True)
        assert self.dataset.return_single_data_record(1, ('fake_extractor',))['fake_extractor'] == 46
        self.dataset.extract_feature(fake_extractor, 'meaning of life', overwrite_feature=True)
        assert self.dataset.return_single_data_record(1, ('fake_extractor',))['fake_extractor'] == 'meaning of life'

    def test_append_data_record(self):
        current_records = self.dataset.total_records
        self.dataset.append_data_record({'fake_extractor': 'NOT THE MEANING OF LIFE AT ALL'})
        assert self.dataset.total_records == current_records + 1
        assert self.dataset.return_single_data_record(current_records + 1, ('fake_extractor',))['fake_extractor']\
               == 'NOT THE MEANING OF LIFE AT ALL'

    def test_insert_single_value(self):
        self.dataset.insert_single(5, 'extra_field', 'extra number')
        assert self.dataset.feature_exists(5, 'extra_field') is True
        assert self.dataset.feature_exists(4, 'extra_field') is False

    def test_delete_feature(self):
        self.dataset.extract_feature(a_very_fake_extractor, overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True
        self.dataset.delete_feature('a_very_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False

    def test_rename_feature(self):
        self.dataset.extract_feature(a_very_fake_extractor, overwrite_feature=True)
        self.dataset.extract_feature(ext3, overwrite_feature=True)
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is True
        self.dataset.rename_feature('a_very_fake_extractor', 'renamed_fake_extractor')
        assert self.dataset.feature_exists_global('a_very_fake_extractor') is False
        assert self.dataset.feature_exists_global('renamed_fake_extractor') is True
        assert self.dataset.return_single_data_record(3, ('ext3',))['ext3'] == 23930
        self.dataset.rename_feature('renamed_fake_extractor', 'ext3', overwrite_existing=True, writeback=3)
        assert self.dataset.feature_exists_global('renamed_fake_extractor') is False
        assert self.dataset.feature_exists_global('ext3') is True
        assert self.dataset.return_single_data_record(3, ('ext3',))['ext3'] == 30239

    def test_feature_names(self):
        self.dataset.extract_feature(a_very_fake_extractor, overwrite_feature=True)
        self.dataset.extract_feature(numpy_feature, overwrite_feature=True)
        feature_names = self.dataset.return_feature_names()
        names = []
        lengths = []
        for i in feature_names:
            names.append(i[0])
            lengths.append(i[1])
        assert 'a_very_fake_extractor' in names
        assert 'numpy_feature' in names
        fake_index = names.index('a_very_fake_extractor')
        numpy_index = names.index('numpy_feature')
        assert lengths[fake_index] == 0
        assert lengths[numpy_index] == 3


if __name__ == '__main__':
    unittest.main()