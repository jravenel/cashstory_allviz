# coding=utf-8
import pandas as pd
import pdb
from lib import datagenerator
import os


def augment(dfs):
    """
    Insert here code to augment your data frames during preprocessing
    """

    # Add domain to each dataframe
    for domain, df in dfs.iteritems():
        df['domain'] = domain

    return dfs
