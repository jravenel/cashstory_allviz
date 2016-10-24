# -*- coding: utf-8 -*-
import pandas as pd
from data_frame_preprocessor import DataFramePreprocessor


def augment(dfs):
    preprocessor = DataFramePreprocessor(dfs)
    preprocessor.preprocess()
    dfs_preprocessor = preprocessor.get_preprocessed_data_frames()
    dfs_preprocessor['malls_loc_Caen'] = dfs['malls_loc_Caen']
    dfs_preprocessor['iris_data_Caen'] = dfs['iris_data_Caen']
    return dfs_preprocessor
