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

    file_path = os.path.dirname(os.path.abspath(__file__))
    small_app_id = os.path.basename(os.path.abspath(os.path.join(file_path, os.pardir)))
    generator = datagenerator.builder(small_app_id)

    ### Example
    labels = {
        'Par Zone': ['Calvaldos','Loire Atlantique','Isère','Alpes Maritime','Picardie','Manche','Orne','Mozelle'],
        'Par PDV': ['Caen','Rennes','Nantes','Lyon','Lille']
        }
    categories = ['Toutes zones rangées','Au moins 1 zone pas propre','Toutes zones sales']
    breakdown = ['Par Zone','Par PDV']
    generator( {'stack':True}, labels, categories, breakdown, 'test_generator')


    return dfs
