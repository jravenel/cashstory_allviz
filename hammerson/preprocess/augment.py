# coding=utf-8
import pandas as pd
import numpy as np
import pdb
from lib import datagenerator
import datetime as dt
from datetime import datetime
from pandas import DataFrame, Series
import os


def augment(dfs):
    """
    Insert here code to augment your data frames during preprocessing
    """

    shopping_centers = ['QT']
    months = ['2014_06','2014_07','2014_08','2014_09','2014_10','2014_11','2014_12','2015_01','2015_02','2015_03','2015_04','2015_05','2015_06','2015_07','2015_08','2015_09','2015_10','2015_11','2015_12','2016_01','2016_02','2016_03','2016_04','2016_05','2016_06','2016_07','2016_08','2016_09']
    sheetformat_tenant = '{month}'
    sheetformat_center = 'KPI {sc}'
    keyformat_center = 'full_{sc}'
    keyformat_jointure = 'jointure_{sc}'
    dfs['center_kpis'] = []
    dfs['tenants_kpis'] = []
    dfs['last_month_kpis'] = []
    dfs['last_month_kpis_average'] = []
    dfs['jointure'] = []

    def adjustPrecision(df, column, precision):
      stf = "{:."+str(precision)+"f}"
      df[column] = df[column].apply(lambda x: stf.format(x) )

    def processCenter(sc,sc_id, sc_name):
      center_kpis = dfs['global_CS_QT'][sheetformat_center.format(sc=sc)].iloc[0]
      for month in months:
        keycenter = keyformat_center.format(sc=sc)
        keyjointure = keyformat_jointure.format(sc=sc)
        sheetname = sheetformat_tenant.format(sc=sc,month=month)
        all_kpis_sheet = dfs[keycenter][sheetname]
        all_kpis_sheet[all_kpis_sheet['exclusion'] != 1]
        #center_kpis['Total sales'] = all_kpis_sheet['Sales R12M'].sum()
        print month, sc
        all_kpis_sheet = all_kpis_sheet.replace('NA',np.nan).replace(' NA ',np.nan).replace(' NC ',np.nan).replace('NC',np.nan).replace('exc',np.nan)
        center_kpis['Average Purchase conversion rate'] = all_kpis_sheet['Purchase conversion rate'].mean()
        center_kpis['Average Shopfront conversion rate'] = all_kpis_sheet['Shopfront conversion rate'].mean()
        #all_kpis_sheet = all_kpis_sheet.fillna(0).replace('NA',0).replace(' NA ',0).replace(' NC ',0).replace('NC',0)
        #all_kpis_sheet['Average basket'] = all_kpis_sheet['Average basket'].astype(float)
        all_means = all_kpis_sheet.groupby('branch level 3').mean()
        all_kpis_sheet = pd.merge(all_kpis_sheet,all_means, left_on="branch level 3", right_index=True, suffixes=('','_comp'))
        all_kpis_sheet['center_id'] = str(sc_id)
        all_kpis_sheet['center'] = sc_name
        all_kpis_sheet['month'] = month
        all_kpis_sheet['retailer_name'] = all_kpis_sheet['Retailer Name']
        all_kpis_sheet['branch_level_2'] = all_kpis_sheet['branch level 2']

        all_kpis_sheet['lease_end'] =  pd.to_datetime(all_kpis_sheet['Reversionary date'],errors='coerce').dt.strftime('%Y-%m-%d')
        all_kpis_sheet['opening_date'] = pd.to_datetime(all_kpis_sheet['Opening date'],errors='coerce').dt.strftime('%Y-%m-%d')
        dfs['tenants_kpis'].append(all_kpis_sheet)

        if month == months[-1]:
          last_month_kpis = all_kpis_sheet


          jointure_store_shape = dfs[keyjointure]['Store label']
          jointure_ent = dfs[keyjointure]['Entity Handle'][['Shape_ID', 'EntityHand','floor']]
          jointure = pd.merge(jointure_ent,jointure_store_shape, on="Shape_ID")

          jointure['EntityHand'] = jointure['EntityHand'].apply(lambda x: str(x).split('.')[0])
          jointure = jointure.rename(columns={'Retailer name': 'Retailer Name'})

          columns = {u'Instore traffic_comp': u'Instore Traffic_comp',
               'Shopfront traffic (customer visit count)':'Shop Front Traffic',
               'Shopfront traffic (customer visit count)_comp':'Shop Front Traffic_comp',
               'Shopfront conversion rate':'Shop Front Conversion',
               'Shopfront conversion rate_comp':'Shop Front Conversion_comp',
               u'Instore traffic': u'Instore Traffic',
               u'% of destination traffic': u'Destination Traffic',
               u'% of destination traffic_comp': u'Destination Traffic_comp',
               u'Current OCR': u'OCR',
               u'Current OCR_comp': u'OCR_comp',
               u'Sales R12M/ m²': u'Sales/m²',
               u'Sales R12M/ m²_comp': u'Sales/m²_comp',
               u'MGR/m² (€/m²)': u'MGR/m²',
               u'MGR/m² (€/m²)_comp': u'MGR/m²_comp',
               u'ERV/m² (€/m²)': u'ERV/m²',
               u'ERV/m² (€/m²)_comp': u'ERV/m²_comp',
               u'Reversionary date':'Lease end',
               u'Sales month/ Net Shopping Hours':'Sales/NSH',
               u'Reversionary Potential (€)':'Reversionary Potential',
               u'connected retailer 1':'connected_retailer_1',
               u'connected retailer 2':'connected_retailer_2',
               u'connected retailer 3':'connected_retailer_3',
               u'connected retailer 4':'connected_retailer_4',
               u'connected retailer 5':'connected_retailer_5',
               u'Total GLA (m²)': 'GLA'}

          last_month_kpis = last_month_kpis.rename(columns=columns)
          all_means_2 = last_month_kpis.groupby('branch level 2').mean()
          all_means_2 = all_means_2.reset_index().rename(columns=columns)

          last_month_kpis['Destination Traffic'] *= 100
          last_month_kpis['Destination Traffic_comp'] *= 100
          last_month_kpis['Shop Front Conversion'] *= 100
          last_month_kpis['Shop Front Conversion_comp'] *= 100
          last_month_kpis['OCR'] *= 100
          last_month_kpis['OCR_comp'] *= 100
          last_month_kpis['Dwell time'] *= 60
          last_month_kpis['Dwell time_comp'] *= 60

          def toto(x):
            if pd.isnull(x) or not isinstance(x, dt.date):
              return 'no Lease end'
            else:
              return x.strftime( '%Y/%m/%d')

          last_month_kpis['Lease end'] = last_month_kpis['Lease end'].apply(toto)
          last_month_kpis['Purchase conversion rate'] *= 100
          last_month_kpis['Awareness rate'] *= 100
          last_month_kpis['Sales R12M'] /= 1000000
          last_month_kpis['Sales R12M_comp'] /= 1000000

          #with open('average-{sc_id}.json'.format(sc_id=sc_id), mode='w') as outfile:
            #meanssss = last_month_kpis.mean().astype(int)
            #json.dump(meanssss.to_dict(), outfile)

          all_means_2['Destination Traffic'] *= 100
          all_means_2['Shop Front Conversion'] *= 100
          all_means_2['OCR'] *= 100
          all_means_2['Dwell time'] *= 60

          adjustPrecision(all_means_2, 'Instore Traffic',0)
          adjustPrecision(all_means_2, 'Destination Traffic',1)
          adjustPrecision(all_means_2, 'Shop Front Conversion',1)
          adjustPrecision(all_means_2, 'Shop Front Traffic',0)
          adjustPrecision(all_means_2, u'Sales/m²',0)
          adjustPrecision(all_means_2, u'MGR/m²',0)
          adjustPrecision(all_means_2, u'ERV/m²',0)
          adjustPrecision(all_means_2, 'OCR',1)
          adjustPrecision(all_means_2, 'Dwell time',0)
          adjustPrecision(all_means_2, 'Sales/NSH',0)

          adjustPrecision(last_month_kpis, 'Reversionary Potential',0)
          adjustPrecision(last_month_kpis, 'Median income',0)
          adjustPrecision(last_month_kpis, 'Awareness rate',0)
          adjustPrecision(last_month_kpis, 'Average basket',0)
          adjustPrecision(last_month_kpis, 'Average basket_comp',0)
          adjustPrecision(last_month_kpis, 'Instore Traffic',0)
          adjustPrecision(last_month_kpis, 'Instore Traffic_comp',0)
          adjustPrecision(last_month_kpis, 'Destination Traffic',1)
          adjustPrecision(last_month_kpis, 'Destination Traffic_comp',1)
          adjustPrecision(last_month_kpis, 'Shop Front Conversion',1)
          adjustPrecision(last_month_kpis, 'Shop Front Conversion_comp',1)
          adjustPrecision(last_month_kpis, 'Shop Front Traffic',0)
          adjustPrecision(last_month_kpis, 'Shop Front Traffic_comp',0)
          adjustPrecision(last_month_kpis, u'Sales/m²',0)
          adjustPrecision(last_month_kpis, u'Sales/m²_comp',0)
          adjustPrecision(last_month_kpis, u'MGR/m²',0)
          adjustPrecision(last_month_kpis, u'MGR/m²_comp',0)
          adjustPrecision(last_month_kpis, u'ERV/m²',0)
          adjustPrecision(last_month_kpis, u'ERV/m²_comp',0)
          adjustPrecision(last_month_kpis, 'OCR',1)
          adjustPrecision(last_month_kpis, 'OCR_comp',1)
          adjustPrecision(last_month_kpis, 'Dwell time',0)
          adjustPrecision(last_month_kpis, 'Dwell time_comp',0)
          adjustPrecision(last_month_kpis, 'Sales R12M',2)
          adjustPrecision(last_month_kpis, 'Sales R12M_comp',2)
          adjustPrecision(last_month_kpis, 'GLA',0)
          adjustPrecision(last_month_kpis, 'Sales/NSH',0)

          last_month_kpis = last_month_kpis.drop_duplicates(subset=['Retailer Name'])
          last_month_kpis['center_id'] = str(sc_id)
          jointure['center_id'] = str(sc_id)
          jointure['center'] = sc_name
          all_means_2['center_id'] = str(sc_id)
          all_means_2['center'] = sc_name
          dfs['last_month_kpis'] = last_month_kpis
          dfs['last_month_kpis_average'] = all_means_2
          dfs['jointure'] = jointure

      center_kpis['center_id'] = str(sc_id)
      center_kpis['center'] = sc_name
      dfs['center_kpis'] = DataFrame(center_kpis).transpose()

    #processCenter('CS',0)
    processCenter('QT',1, 'Les 3 Fontaines')
    dfs['tenants_kpis'] = pd.concat(dfs['tenants_kpis'])

    del dfs['full_CS']
    del dfs['full_QT']
    del dfs['global_CS_QT']
    del dfs['jointure_CS']
    del dfs['jointure_QT']

    def do_carto():
      jointure = dfs['jointure']

      last_month_kpis = dfs['last_month_kpis']
      last_month_kpis = pd.merge(jointure,last_month_kpis, on='Retailer Name', suffixes=('','_2'))

      id_vars = [ u'branch level 2',
        u'Retailer Name',
        u'GLA',
        u'EntityHand',
        u'center',
        u'Lease end',
        u'Shape_ID',
        u'Instore Traffic',
        u'floor'
        ]

      def cartoDf(last_month_kpis, id_vars, value_vars,value_vars_comp, subdomain):
        df_1 = pd.melt(last_month_kpis,id_vars,value_vars)
        df_1_comp = pd.melt(last_month_kpis,id_vars,value_vars_comp)
        df_1['subdomain'] = subdomain
        df_1['value_comp'] = df_1_comp['value']
        df_1['floor'] = df_1['floor'].apply(lambda x: 'Floor {}'.format(x))
        df_1 = df_1[~((df_1['EntityHand'] == '73') & (df_1['floor'] == 'Floor 1'))]
        return df_1

      value_vars = [
        u'Sales/m²',
        u'Average basket'
      ]
      value_vars_comp = [
        u'Sales/m²_comp',
        u'Average basket_comp'
      ]
      operational = cartoDf(last_month_kpis, id_vars, value_vars, value_vars_comp, 'operational')
      value_vars = [
       'OCR',
       u'ERV/m²',
       u'MGR/m²',
      ]
      value_vars_comp = [
       'OCR_comp',
       u'ERV/m²_comp',
       u'MGR/m²_comp'
      ]
      finance = cartoDf(last_month_kpis, id_vars, value_vars, value_vars_comp, 'finance')
      value_vars = [
        'Shop Front Traffic',
        'Shop Front Conversion',
        'Destination Traffic'
      ]
      value_vars_comp = [
        'Shop Front Traffic_comp',
        'Shop Front Conversion_comp',
        'Destination Traffic_comp'
      ]
      attracivity = cartoDf(last_month_kpis, id_vars, value_vars, value_vars_comp, 'attracivity')

      res = [operational,finance,attracivity]
      return pd.concat(res)

    dfs['carto'] = do_carto()


    def do_bl3_comp():
      all_means_2 = dfs['last_month_kpis_average']

      id_vars = [ u'branch level 2','center']
      value_vars = [
          #'Average basket',
          u'Sales/NSH',
          u'Sales/m²',
          #u'Purchase conversion rate'
          ]

      df_1 = all_means_2[(id_vars + value_vars)]
      df_1  = pd.melt(df_1,id_vars = id_vars, value_vars=value_vars)
      df_1['subdomain'] = 'operational'

      value_vars = [
        u'Destination Traffic',
        u'Instore Traffic',
        u'Shop Front Traffic',
        u'Shop Front Conversion']

      df_2 = all_means_2[(id_vars + value_vars)]
      df_2 = pd.melt(df_2,id_vars = id_vars, value_vars=value_vars)
      df_2['subdomain'] = 'attracivity'

      value_vars = [
          u'Dwell time',
          'OCR'
          ]

      df_3 = all_means_2[(id_vars + value_vars)]
      df_3 = pd.melt(df_3,id_vars = id_vars, value_vars=value_vars)
      df_3['subdomain'] = 'time'


      res = [df_1,df_2,df_3]
      return pd.concat(res)

    dfs['bl3_comp'] = do_bl3_comp()


    def do_cat_anal():
      last_month_kpis = dfs['last_month_kpis']

      id_vars = [ u'branch level 2',
        u'Retailer Name',
        u'center',
        u'GLA',
        u'Lease end']

      value_vars = [
        u'Destination Traffic',
        u'Instore Traffic',
        u'Shop Front Traffic',
        u'Shop Front Conversion']

      df = last_month_kpis[(id_vars + value_vars)]
      df_1 = pd.melt(last_month_kpis,id_vars = id_vars, value_vars=value_vars)
      df_1['subdomain'] = 'attracivity'

      value_vars = [u'Dwell time']

      df = last_month_kpis[(id_vars + value_vars)]
      df_2 = pd.melt(last_month_kpis,id_vars = id_vars, value_vars=value_vars)
      df_2['subdomain'] = 'time'

      value_vars = [
          #'Average basket',
          u'Sales R12M',
          u'Sales/m²',
          u'Sales/NSH',
          #u'Purchase conversion rate'
          ]

      df = last_month_kpis[(id_vars + value_vars)]
      df_3 = pd.melt(last_month_kpis,id_vars = id_vars, value_vars=value_vars)
      df_3['subdomain'] = 'operational'

      value_vars = [
          u'ERV/m²',
          u'MGR/m²',
          u'OCR',
          u'Reversionary Potential'
          ]

      df = last_month_kpis[(id_vars + value_vars)]
      df_4 = pd.melt(last_month_kpis,id_vars = id_vars, value_vars=value_vars)
      df_4['subdomain'] = 'finance'

      value_vars = [
          u'Awareness rate',
          u'Median age',
          u'Median income'
          ]

      df = last_month_kpis[(id_vars + value_vars)]
      df_5 = pd.melt(last_month_kpis,id_vars = id_vars, value_vars=value_vars)
      df_5['subdomain'] = 'socio'


      res = [df_1,df_2,df_3,df_4,df_5]
      return pd.concat(res)

    dfs['cat_anal'] = do_cat_anal()


    def do_center(center_id):
      cursor = db.slideData.find({'domain': 'last_month_kpis', 'center_id':str(center_id)})
      last_month_kpis = DataFrame(list(cursor))
      res = []
      for row in last_month_kpis.iterrows():
        store_label = row[1]['Retailer Name']
        cursor = db.slideData.find({'domain': 'tenants_kpis', 'center_id': str(center_id), 'Retailer Name': store_label })
        store_history = DataFrame(list(cursor))

        sales = DataFrame(store_history[['month','Sales R12M','Retailer Name','center_id']])
        sales['Sales R12M'] /= 1000000
        sales['center_id'] = str(center_id)
        sales['id'] = 21
        sales = sales.dropna(subset=['Sales R12M'])

        shop = DataFrame(store_history[['month','Shopfront traffic (customer visit count)','Retailer Name','center_id']])
        #shop['Shopfront traffic (customer visit count)'] = shop['Shopfront traffic (customer visit count)'].astype(int)
        shop['center_id'] = str(center_id)
        shop['id'] = 1
        shop = shop.dropna(subset=['Shopfront traffic (customer visit count)'])

        shop_ = DataFrame(store_history[['month','Shopfront traffic as % of total SC traffic','Retailer Name','center_id']])
        shop_['Shopfront traffic as % of total SC traffic'] = 100*shop_['Shopfront traffic as % of total SC traffic']
        shop_['id'] = 2
        shop_['center_id'] = str(center_id)
        shop_ = shop_.dropna(subset=['Shopfront traffic as % of total SC traffic'])

        instore = DataFrame(store_history[['month','Instore traffic','Retailer Name','center_id']])
        instore['center_id'] = str(center_id)
        instore['id'] = 3
        instore = instore.dropna(subset=['Instore traffic'])

        if sales.shape[0] > 0:
          res.append(sales)
        if shop.shape[0] > 0:
          res.append(shop)
        if shop_.shape[0] > 0:
          res.append(shop_)
        if instore.shape[0] > 0:
          res.append(instore)
      return res

    ###### DASHBOARDS
#    month = '2015_09'
#    def do_center(center_id):
#      tenants_dashboards = []
#      cursor = db.slideData.find({'domain':'tenants_kpis','center_id': str(center_id), 'month': month})
#      all_stores_kpis = DataFrame(list(cursor))
#      for index, store_kpis in all_stores_kpis.iterrows():
#        store_label = store_kpis['Retailer Name']
#        slug = slugify(store_label)
#        bl3 = store_kpis['branch level 2']
#
#        overrides = {'month': month, 'entityName': store_kpis['Retailer Name']}
#        overrides['name'] = store_label
#        overrides['reportId'] = center_id
#        overrides['slug'] = slug
#        overrides['entityName'] = store_label
#        #overrides['entityGroup'] = bl3
#        overrides['attributes'] = []
#        first_row_attributes = []
#        if pd.isnull(store_kpis[u'Total GLA (m²)']):
#          first_row_attributes.append({'value': 'NA '})
#        else:
#          first_row_attributes.append({'value': '{0}m² '.format( int(store_kpis[u'Total GLA (m²)']) ) })
#
#        first_row_attributes.append({'value': bl3})
#        first_row_attributes.append({'value': store_kpis['Zone name / Level name']})
#        if pd.isnull(store_kpis[u'Opening date']):
#          first_row_attributes.append({'value': ' Opened since NA'})
#        else:
#          first_row_attributes.append({'value': ' Opened since {0}'.format(datetime.fromtimestamp( store_kpis['Opening date']/1000).strftime('%d/%m/%Y')) if isinstance(store_kpis['Opening date'],(int,float))  else 'NA'  })
#
#        if pd.isnull(store_kpis[u'Reversionary date']):
#          first_row_attributes.append({'value': ' Reversionary date: NA'})
#        else:
#          first_row_attributes.append({'value': ' Reversionary date {0}'.format( datetime.fromtimestamp(store_kpis['Reversionary date']/1000).strftime('%d/%m/%Y') if (isinstance(store_kpis['Reversionary date'],(int,float,long)) and store_kpis['Reversionary date'] != 0) else 'NA')})
#        overrides['attributes'].append(first_row_attributes)
#        if db.slideData.find({'domain':'tenants_kpis','Retailer Name': store_label, 'month': month}).count() > 1:
#          sc_name = 'Carré Sénart' if id == 1 else 'Les 4 Temps'
#          overrides['attributes'].append([{'name':'Also present in ', 'value':'<a>{sc_name}</a>'.format(sc_name=sc_name)}])
#        tenants_dashboards.append(generate(index+301*center_id,center_id, 'tenant-dashboard', overrides))
#
#      return tenants_dashboards
#
#
#    if template == 'center-dashboard':
#      print 'doing center'
#      #if 'slug' not in dashboard_data:
#        #report_data['slug'] = slugify(dashboard_data['entityName'] + ' ' + dashboard_data['date'])
#
#      cursor = db.slideData.find({'domain': 'center_kpis','center_id': str(report)})
#      center_kpis = DataFrame(list(cursor)).ix[0]
#
#      dashboard.updateKpi(id=4, value=center_kpis['Total annual footfall']/1000000., value_comp=center_kpis[u'Total annual footfall evolution'], sentiment = center_kpis[u'Total annual footfall evolution']>0)
#      dashboard.updateKpi(id=5, value=center_kpis['monthly footfall']/1000000.)
#      dashboard.updateKpi(id=6, value=center_kpis['Dwell time']/60)
#      dashboard.updateKpi(id=7, value=center_kpis['% of first time visitors'])
#      dashboard.updateKpi(id=8, value=center_kpis['Visit frequency per month'])
#      dashboard.updateKpi(id=9, value=center_kpis['Average number of visited tenants'])
#
#      dashboard.updateKpi(id=16, value=center_kpis['Avg marketing events attendance'])
#
#      dashboard.updateKpi(id=10, value=center_kpis[u'% of women'])
#      dashboard.updateKpi(id=11, value=center_kpis[u'% of CSP+'])
#      dashboard.updateKpi(id=12, value=center_kpis[u'Median age'])
#      dashboard.updateKpi(id=13, value=center_kpis[u'Median income'])
#
#      dashboard.updateKpi(id=0, value=center_kpis[u'Total sales'], value_comp=center_kpis[u'Total sales evolution'], sentiment = center_kpis[u'Total sales evolution']>0)
#      dashboard.updateKpi(id=1, value=center_kpis[u'Average Shopfront conversion rate'])
#      dashboard.updateKpi(id=2, value=(center_kpis[u'Average Purchase conversion rate'] or 0))
#
#      dashboard.updateKpi(id=14, value=center_kpis[u'Awareness rate of shopping center'])
#      dashboard.updateKpi(id=15, value=center_kpis[u'Penetration rate of shopping center'])
#
#
#    if template == 'tenant-dashboard':
#      cursor = db.slideData.find({
#          'domain':'tenants_kpis'
#         ,'center_id': str(report)
#         ,'month': overrides['month']
#         ,'Retailer Name': overrides['entityName']
#         })
#      store_kpis = DataFrame(list(cursor)).ix[0]
#      print 'doing tenant'
#
#      dashboard.updateKpi(
#          id=1,
#          value=store_kpis['Shopfront traffic (customer visit count)'],
#          value_comp=store_kpis['Shopfront traffic (customer visit count)_comp']
#          )
#      dashboard.updateKpi(
#          id=2,
#          value= store_kpis['Shopfront traffic as % of total SC traffic']
#      )
#      dashboard.updateKpi(
#          id=3,
#          value=store_kpis['Instore traffic'],
#          value_comp=store_kpis['Instore traffic_comp']
#          )
#      dashboard.updateKpi(
#          id=4,
#          value=store_kpis['Shopfront conversion rate'],
#          value_comp=store_kpis['Shopfront conversion rate_comp'],
#          )
#      dashboard.updateKpi(
#          id=5,
#          value=60*store_kpis['Dwell time'] if not pd.isnull(store_kpis['Dwell time']) else store_kpis['Dwell time'],
#          value_comp=60*store_kpis['Dwell time_comp'] if not pd.isnull(store_kpis['Dwell time']) else store_kpis['Dwell time'],
#          )
#      dashboard.updateKpi(
#          id=6,
#          value=store_kpis['Visit frequency'],
#          value_comp=store_kpis['Visit frequency_comp']
#          )
#
#      dashboard.updateKpi(
#          id=7,
#          value=store_kpis['% of >20 min visitors'],
#          value_comp=store_kpis['% of >20 min visitors_comp']
#          )
#      dashboard.updateKpi(
#          id=8,
#          value=store_kpis['% of first time visitors'],
#          value_comp=store_kpis['% of first time visitors_comp']
#          )
#      dashboard.updateKpi(
#          id=9,
#          value=store_kpis['% of destination traffic'] ,
#          value_comp=store_kpis['% of destination traffic_comp']
#          )
#      dashboard.updateKpi(
#          id=10,
#          value=store_kpis['% of exclusive traffic'],
#          value_comp=store_kpis['% of exclusive traffic_comp']
#          )
#      dashboard.updateKpi(
#          id=11,
#          value=store_kpis['% of visitors visiting mainly the tenant'] ,
#          value_comp=store_kpis['% of visitors visiting mainly the tenant_comp']
#          )
#      dashboard.updateKpi(
#          id=12,
#          value=store_kpis['% of frequent visitors'],
#          value_comp=store_kpis['% of frequent visitors_comp']
#          )
#      dashboard.updateKpi(
#          id=13,
#          value=store_kpis['Average number of store visits'],
#          value_comp=store_kpis['Average number of store visits_comp']
#          )
#      dashboard.updateKpi(
#          id=14,
#          value=u'{0}, {1}, {2}, {3}, {4}'.format(unicode(store_kpis['connected retailer 1'])
#            ,unicode(store_kpis['connected retailer 2'])
#            ,unicode(store_kpis['connected retailer 3'])
#            ,unicode(store_kpis['connected retailer 4'])
#            ,unicode(store_kpis['connected retailer 5'])
#            )
#          )
#      dashboard.updateKpi(
#          id=15,
#          value=store_kpis[u'% of women'] ,
#          value_comp=store_kpis[u'% of women_comp']
#          )
#      dashboard.updateKpi(
#          id=16,
#          value=store_kpis[u'% of CSP+'],
#          value_comp=store_kpis[u'% of CSP+_comp']
#          )
#      dashboard.updateKpi(
#          id=17,
#          value=store_kpis[u'Median age'] ,
#          value_comp=store_kpis[u'Median age_comp']
#          )
#      dashboard.updateKpi(
#          id=18,
#          value=store_kpis[u'Median income'] ,
#          value_comp=store_kpis[u'Median income_comp']
#          )
#      dashboard.updateKpi(
#          id=19,
#          value=store_kpis[u'Awareness rate'] ,
#          value_comp=store_kpis[u'Awareness rate_comp']
#          )
#      dashboard.updateKpi(
#          id=20,
#          value=store_kpis[u'Shop challenge score'],
#          value_comp=store_kpis[u'Shop challenge score_comp']
#          )
#      dashboard.updateKpi(
#          id=21,
#          value=store_kpis['Sales R12M']/1000000 if isinstance(store_kpis['Sales R12M'], (int, long, float) ) else 'NA'
#          )
#      dashboard.updateKpi(
#          id=22,
#          value=store_kpis['Sales R12M evolution'],
#          value_comp=store_kpis['Sales R12M evolution_comp'],
#          )
#      dashboard.updateKpi(
#          id=23,
#          value=store_kpis[u'Sales R12M/ m²'] ,
#          value_comp=store_kpis[u'Sales R12M/ m²_comp'] ,
#          )
#      dashboard.updateKpi(
#          id=24,
#          value=store_kpis['Average basket'] ,
#          value_comp=store_kpis['Average basket_comp']
#          )
#      dashboard.updateKpi(
#          id=25,
#          value=store_kpis['Purchase conversion rate'],
#          value_comp=store_kpis['Purchase conversion rate_comp']
#          )
#      dashboard.updateKpi(
#          id=26,
#          value=store_kpis['Sales month/ Net Shopping Hours'] ,
#          value_comp=store_kpis['Sales month/ Net Shopping Hours_comp']
#          )
#      dashboard.updateKpi(
#          id=27,
#          value=store_kpis[u'MGR/m² (€/m²)'],
#          value_comp=store_kpis[u'MGR/m² (€/m²)_comp']
#      )
#      dashboard.updateKpi(
#          id=28,
#          value=store_kpis[u'SBR/m² (€/m²)']
#          )
#      dashboard.updateKpi(
#          id=29,
#          value=store_kpis[u'Service charges/m² (€/m²)']
#          )
#      dashboard.updateKpi(
#          id=30,
#          value=store_kpis[u'Current OCR'],
#          value_comp=store_kpis[u'Current OCR_comp'],
#          reverse=True
#          )
#      dashboard.updateKpi(
#          id=31,
#          value=store_kpis[u'ERV/m² (€/m²)'],
#          value_comp=store_kpis[u'ERV/m² (€/m²)_comp'],
#          )
#      dashboard.updateKpi(
#          id=32,
#          value=store_kpis[u'Reversionary Potential (€)']
#          )
#      dashboard.updateDataQuery(id=21,key='Retailer Name',value=overrides['entityName'])
#      dashboard.updateDataQuery(id=1,key='Retailer Name',value=overrides['entityName'])
#      dashboard.updateDataQuery(id=2,key='Retailer Name',value=overrides['entityName'])
#      dashboard.updateDataQuery(id=3,key='Retailer Name',value=overrides['entityName'])

    return dfs
