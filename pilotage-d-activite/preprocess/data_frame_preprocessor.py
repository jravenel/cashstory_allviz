# -*- coding: utf8 -*-
import pandas as pd


class DataFramePreprocessor(object):

    TIME_SERIES_END_MONTH = 8

    TIME_SERIES_END_YEAR = 2015

    TIME_SERIES_START_MONTH = 8

    TIME_SERIES_START_YEAR = 2014

    EARLIEST_DATA_AVAILABILITY_DATE = '08/2014'

    LATEST_DATA_AVAILABILITY_DATE = '08/2015'

    LATEST_MONTH = u'Août'

    def __init__(self, data_frames):
        require_normalization = [
            'turnover',
            'turnover_progress',
            'average_basket_progress',
            'article_progress',
            'customer_progress',
            'customer_article_progress',
            #'average_price_progress',
            'net_margins',
            'raw_cash_flow',
            'net_cash_flow',
            'staff_fees',
            'immutable_fees',
            'markdown',
            'mutable_fees',
            'known_demarques',
            'unknown_demarques',
            'subscription_fees',
            'global_margins',
            'taux_transport',
            'impots',
            'participation',
            'disbursed_fees'
        ]

        for domain in require_normalization:
            data_frames[domain] = self.normalize_data_frame(data_frames[domain])

        self.data_frames = data_frames

        self.preprocessed_data_frames = {}
        self.growths = []
        self.raw_cash_flow = []
        self.companies_metrics = []
        self.france_metrics = []
        self.territories_metrics = []
        self.latest_metrics = []
        self.ranking_frames = []

        self.current_parent_zone = None
        self.turnover_progress = None

        pd.options.mode.chained_assignment = 'raise'

    def preprocess(self):
        self.preprocess_dashboards()
        self.preprocess_raw_cash_flow()
        self.preprocess_net_cash_flow()
        self.preprocess_marge_nette()
        self.preprocess_synthese()
        self.preprocess_stacked_metrics()
        self.preprocess_ranking()
        self.preprocess_target_metrics()
        self.preprocess_growths()
        self.preprocess_latest_metrics()
        self.preprocess_monthly_metrics()
        self.preprocess_turnover()

    def normalize_data_frame(self, df):
        df = df.replace({'Label': self.get_companies_substitutions()})
        return df.replace({'Date': self.get_date_substitutions()})

    @classmethod
    def get_companies_substitutions(cls):
        return {'NORD EST': 'Nord-Est', 'SUD ': 'Sud', 'OUEST': 'Ouest', 'OUEST ': 'Ouest', 'France': 'TOTAL France'}

    @classmethod
    def get_date_substitutions(cls):
        return {u'Juillet': '07/2015', u'Août': '08/2015', u'Septembre': '09/2015'}

    def preprocess_marge_nette(self):
        column_suffix = ' cumul'

        label = 'Marge globale'
        marge_globale = self.preprocess_waterfall_bar(
            self.data_frames['global_margins'],
            level=label,
            sublevel=label,
            rename_column=label + column_suffix
        )

        label = 'Marge nette'
        marge_nette = self.preprocess_waterfall_bar(
            self.data_frames['net_margins'],
            level=label,
            sublevel=label,
            rename_column=label + column_suffix
        )

        label = u'Démarques connues'
        known_demarques = self.preprocess_waterfall_bar(
            self.data_frames['known_demarques'],
            level=u'Démarques',
            sublevel=label,
            rename_column=label + column_suffix,
            factor=-1
        )

        label = u'Démarques inconnues'
        unknown_demarques = self.preprocess_waterfall_bar(
            self.data_frames['unknown_demarques'],
            level=u'Démarques',
            sublevel=label,
            rename_column=label + column_suffix,
            factor=-1
        )
        label = self.get_transport_rate_label()
        taux_transport = self.preprocess_waterfall_bar(
            self.data_frames['taux_transport'],
            level=self.get_transport_rate_label(),
            sublevel=label,
            rename_column=u'Démarques connnues cumul',
            factor=-1
        )
        self.preprocessed_data_frames['marge_nette'] = pd.concat([
            marge_globale,
            known_demarques,
            unknown_demarques,
            taux_transport,
            marge_nette
        ])

    def get_transport_rate_label(self):
        return u'Taux de transport'

    def preprocess_net_cash_flow(self):
        column_suffix = ' cumul'
        label = 'Cash Flow Brut'
        raw_cash_flow = self.preprocess_waterfall_bar(self.data_frames['raw_cash_flow'],
                                                      level=label,
                                                      sublevel=label,
                                                      rename_column=u'Cash Flow Brut' + column_suffix)
        label = self.get_taxes_label()
        impots = self.preprocess_waterfall_bar(
            self.data_frames['impots'],
            level=label,
            sublevel=label,
            rename_column=u'Impots' +column_suffix,
            factor=1
        )
        label = 'Participation'
        participation = self.preprocess_waterfall_bar(
            self.data_frames['participation'],
            level=label,
            sublevel=label,
            rename_column=u'Participation' +column_suffix,
            factor=-1
        )
        label = 'Cash Flow Net'
        net_cash_flow = self.preprocess_waterfall_bar(self.data_frames['net_cash_flow'],
                                                      level=label,
                                                      sublevel=label,
                                                      rename_column=u'Participation Cumul')

        self.preprocessed_data_frames['net_cash_flow'] = pd.concat([
            raw_cash_flow,
            impots,
            participation,
            net_cash_flow,
        ])

    @classmethod
    def get_taxes_label(cls):
        return 'Impôts'

    def preprocess_synthese(self):
        column_suffix = ' cumul'
        label = 'Marge globale'
        marge_globale = self.preprocess_waterfall_bar(
            self.data_frames['global_margins'],
            level=label,
            sublevel=label,
            rename_column=label + column_suffix
        )


        label = u'Démarques connues'
        known_demarques = self.preprocess_waterfall_bar(
            self.data_frames['known_demarques'],
            level=self.get_markdown_and_transport_rate_label(),
            sublevel=label,
            rename_column=label + column_suffix,
            factor=-1
        )

        label = u'Démarques inconnues'
        unknown_demarques = self.preprocess_waterfall_bar(
            self.data_frames['unknown_demarques'],
            level=self.get_markdown_and_transport_rate_label(),
            sublevel=label,
            rename_column=label + column_suffix,
            factor=-1
        )
        label = self.get_transport_rate_label()
        taux_transport = self.preprocess_waterfall_bar(
            self.data_frames['taux_transport'],
            level=self.get_markdown_and_transport_rate_label(),
            sublevel=label,
            rename_column=u'Démarques connnues cumul',
            factor=-1
        )

        label = self.get_taxes_label()
        impots = self.preprocess_waterfall_bar(
            self.data_frames['impots'],
            level=self.get_taxes_label() + ' & Participation',
            sublevel=label,
            rename_column=u'Impots' +column_suffix,
            factor=1
        )
        label = 'Participation'
        participation = self.preprocess_waterfall_bar(
            self.data_frames['participation'],
            level=self.get_taxes_label() + ' & Participation',
            sublevel=label,
            rename_column=u'Participation' +column_suffix,
            factor=-1
        )
        label = 'Cash Flow Net'
        net_cash_flow = self.preprocess_waterfall_bar(self.data_frames['net_cash_flow'],
                                                      level=label,
                                                      sublevel=label,
                                                      rename_column=u'Participation Cumul')

        label = 'Frais de personnel'
        staff_fees = self.preprocess_waterfall_bar(self.data_frames['staff_fees'],
                                                  level='Frais',
                                                   sublevel=label,
                                                   rename_column='Total des frais de personnel'+ column_suffix,
                                                   factor=-1)

        label = self.get_mutable_fees_label()
        mutable_fees = self.preprocess_waterfall_bar(self.data_frames['mutable_fees'],
                                                      level='Frais',
                                                     sublevel=label,
                                                     rename_column='Total frais influ hors FP'+ column_suffix,
                                                     factor=-1)

        label = 'Frais non influençables hors FP'
        immutable_fees = self.preprocess_waterfall_bar(self.data_frames['immutable_fees'],
                                                      level='Frais',
                                                       sublevel=label,
                                                       rename_column=u'Total des frais non influençables'+ column_suffix,
                                                       factor=-1)

        label = 'Frais de siège et cotisations'
        subscription_fees = self.preprocess_waterfall_bar(self.data_frames['subscription_fees'],
                                                          level='Frais',
                                                          sublevel=label,
                                                          rename_column=u'Frais siège et cotisations'+ column_suffix,
                                                          factor=-1)

        self.preprocessed_data_frames['synthese'] = pd.concat([
            marge_globale,
            known_demarques,
            unknown_demarques,
            taux_transport,
            staff_fees,
            mutable_fees,
            immutable_fees,
            subscription_fees,
            impots,
            participation,
            net_cash_flow
        ])

    def get_markdown_and_transport_rate_label(self):
        return u'Démarques et Tx de transport'

    def preprocess_raw_cash_flow(self):
        column_suffix = ' cumul'
        label = 'Marge nette'
        marge_nette = self.preprocess_waterfall_bar(self.data_frames['net_margins'],
                                                    level=label,
                                                    sublevel=label,
                                                    rename_column=label+ column_suffix)

        label = 'Frais de personnel'
        staff_fees = self.preprocess_waterfall_bar(self.data_frames['staff_fees'],
                                                   level=label,
                                                   sublevel=label,
                                                   rename_column='Total des frais de personnel'+ column_suffix,
                                                   factor=-1)

        label = self.get_mutable_fees_label()
        mutable_fees = self.preprocess_waterfall_bar(self.data_frames['mutable_fees'],
                                                     level=label,
                                                     sublevel=label,
                                                     rename_column='Total frais influ hors FP'+ column_suffix,
                                                     factor=-1)

        label = 'Frais non influençables hors FP'
        immutable_fees = self.preprocess_waterfall_bar(self.data_frames['immutable_fees'],
                                                       level=label,
                                                       sublevel=label,
                                                       rename_column=u'Total des frais non influençables'+ column_suffix,
                                                       factor=-1)

        label = 'Frais de siège et cotisations'
        subscription_fees = self.preprocess_waterfall_bar(self.data_frames['subscription_fees'],
                                                          level=label,
                                                          sublevel=label,
                                                          rename_column=u'Frais siège et cotisations'+ column_suffix,
                                                          factor=-1)
        label = 'Cash Flow Brut'
        raw_cash_flow = self.preprocess_waterfall_bar(self.data_frames['raw_cash_flow'],
                                                          level=label,
                                                          sublevel=label,
                                                          rename_column=u'Cash Flow Brut'+ column_suffix)

        self.preprocessed_data_frames['raw_cash_flow'] = pd.concat([
            marge_nette,
            staff_fees,
            mutable_fees,
            immutable_fees,
            subscription_fees,
            raw_cash_flow,
        ])

    @classmethod
    def get_mutable_fees_label(cls):
        return u'Frais influençables'

    def preprocess_waterfall_bar(self, df, level, sublevel, rename_column='Valeur', factor=1):
        df = df[df['Label'] == 'TOTAL France']
        df = df[df['Date'] == self.get_latest_data_availability_date()]
        df = df.rename(columns={rename_column: 'Value'})
        df['Sublevel Label'] = sublevel
        df['Value'] //= 1000 * factor
        df['Label'] = level
        return df[['Label', 'Sublevel Label', 'Value']]

    def preprocess_stacked_metrics(self):
        sources = {
            'markdown': 'Démarques',
            'staff_fees': 'Frais de personnel',
            'disbursed_fees': 'Frais décaissés',
            'net_margins': 'Marge nette'
        }
        for source, breakdown in sources.iteritems():
            self.companies_metrics.append(self.preprocess_companies_metrics(source, breakdown))
            self.territories_metrics.append(self.preprocess_territories_metrics(source, breakdown))
            self.france_metrics.append(self.preprocess_france_metrics(source, breakdown))


        companies_metrics = pd.concat(self.companies_metrics)
        companies_metrics = companies_metrics.rename(columns={'Value': 'valeur'})

        territories_metrics = pd.concat(self.territories_metrics)
        territories_metrics = territories_metrics.rename(columns={'Value': 'valeur'})

        self.preprocessed_data_frames['companies_metrics'] = companies_metrics
        self.preprocessed_data_frames['territories_metrics'] = territories_metrics
        self.preprocessed_data_frames['france_metrics'] = pd.concat(self.france_metrics)

    def preprocess_companies_metrics(self, source, breakdown):
        """
        Retain "Entreprise"-level records only

        :param source:
        :param breakdown:
        :return:
        """
        companies_metrics = self.data_frames[source]
        companies = ['NORD EST', 'SUD ', 'OUEST ', 'Nord-Est', 'Sud', 'Ouest']
        companies_metrics = companies_metrics[companies_metrics['Label'].isin(companies)]
        substitutions = {'NORD EST': 'Nord-Est', 'SUD ': 'Sud', 'OUEST ': 'Ouest'}
        companies_metrics = companies_metrics.replace({'Label': substitutions})
        return self.preprocess_metrics(breakdown, companies_metrics)

    @classmethod
    def preprocess_metrics(cls, breakdown, companies_metrics):
        companies_metrics = companies_metrics.rename(columns={
            'Valeur': 'Value',
            'Total des frais de personnel': 'Value',
            'Marge nette': 'Value',
            'Prog N-1': 'Growth',
            'Taux Cumul': 'Rate',
            u'Total des frais décaissés': 'Value',
            u'Total des démarques': 'Value'
        })

        if 'Objectifs' in companies_metrics:
            companies_metrics = companies_metrics.drop(['Objectifs'], axis=1)

        companies_metrics['Value'] //= 1000
        companies_metrics['Breakdown'] = breakdown

        return companies_metrics[['Date', 'Label', 'Rate', 'Value', 'Breakdown']]

    def preprocess_territories_metrics(self, source, breakdown):
        territories_metrics = self.data_frames[source]
        territories = ['NORD EST', 'SUD ', 'OUEST ', 'TOTAL France']
        territories_metrics = territories_metrics[False == territories_metrics['Label'].isin(territories)]
        return self.preprocess_metrics(breakdown, territories_metrics)

    def preprocess_france_metrics(self, source, breakdown):
        france_metrics = self.data_frames[source][self.data_frames[source]['Date'].map(self.keep_last_year_metrics)]
        france_label = ['TOTAL France']
        france_metrics = france_metrics[france_metrics['Label'].isin(france_label)]
        return self.preprocess_metrics(breakdown, france_metrics)

    def preprocess_ranking(self):
        frames = {
            'turnover_progress': {
                'rename_columns': {'Prog N-1': 'Growth', 'Turnover': 'Value'},
                'breakdown': 'Chiffre d\'affaires',
                'over_value': 'Turnover'
            },
            'article_progress': {
                'rename_columns': {'Prog n-1': 'Growth', 'Articles': 'Value'},
                'breakdown': 'Articles',
                'over_value': 'Articles'
            },
            'customer_progress': {
                'rename_columns': {'Prog n-1': 'Growth', 'Clients': 'Value'},
                'breakdown': 'Clients',
                'over_value': 'Clients'
            },
            #'customer_article_progress': {
                #'rename_columns': {'Prog n-1': 'Growth', 'Articles par client': 'Value'},
                #'breakdown': 'Articles par client',
                #'over_value': 'Articles par client'
            #},
            #'average_price_progress': {
                #'rename_columns': {'Prog n-1': 'Growth', 'Prix de vente moyen': 'Value'},
                #'breakdown': 'Prix de vente moyen',
                #'over_value': 'Prix de vente moyen'
            #},
            #'average_basket_progress': {
                #'rename_columns': {'Prog n-1': 'Growth', 'Panier moyen': 'Value'},
                #'breakdown': 'Panier moyen',
                #'over_value': 'Panier moyen'
            #}
        }
        for source, properties in frames.iteritems():
            ranking = self.preprocess_ranking_frame(
                source,
                rename_columns=properties['rename_columns'],
                breakdown=properties['breakdown'],
                over_value=properties['over_value']
            )
            self.ranking_frames.append(ranking)

        self.ranking_frames[0]['Value'] = self.ranking_frames[0]['Value'] / 1000000
        self.ranking_frames[2]['Value'] = self.ranking_frames[2]['Value'] / 1000
        #self.ranking_frames[2]['Value'] = self.ranking_frames[2]['Value'] / 1000000
        #self.ranking_frames[3]['Value'] = self.ranking_frames[3]['Value'] / 1000

        self.preprocessed_data_frames['ranking'] = pd.concat(self.ranking_frames)
        self.preprocessed_data_frames['ranking']['Value'] = 8.1*self.preprocessed_data_frames['ranking']['Value']

    def preprocess_ranking_frame(self, source, rename_columns, breakdown, over_value):
        """
        Add Pack values to group horizontal barcharts

        :param source:
        :param rename_columns:
        :param breakdown:
        :param over_value:
        :return:
        """
        ranking = self.pack_growth_frame(source)
        ranking = ranking.rename(columns=rename_columns)

        if source == 'turnover_progress':
            ranking['Value'] /= 1000

        ranking.loc[:, 'Breakdown'] = breakdown
        ranking = self.remove_companies_rows(ranking)

        if self.guard_against_missing_column('Value', ranking, source):
            ranking = ranking.sort(columns=['Value'], ascending=False)

        ranking.loc[:, 'Pack'] = 'Classement'

        sparkline_data_frame = self.data_frames[source]
        renamed_columns = {source: sparkline_data_frame}
        self.rename_data_frames_columns(
            self.get_shared_substitutions(),
            {source: self.data_frames[source]},
            renamed_columns
        )

        self.guard_against_missing_column('Date', renamed_columns[source], source)
        self.guard_against_missing_column('Label', renamed_columns[source], source)
        self.guard_against_missing_column(over_value, renamed_columns[source], source)

        sparklines_ranking = renamed_columns[source][['Label', over_value, 'Date']]
        sparklines_ranking = self.remove_companies_rows(sparklines_ranking)
        sparklines_ranking = self.remove_unavailable_metrics(sparklines_ranking)

        if source == 'turnover_progress':
            sparklines_ranking[over_value] //= 1000

        sparklines_ranking = sparklines_ranking.pivot(index='Label', columns='Date', values=over_value)
        sparklines_ranking = sparklines_ranking.reindex(columns=[
            '08/2014',
            '09/2014',
            '10/2014',
            '11/2014',
            '12/2014',
            '01/2015',
            '02/2015',
            '03/2015',
            '04/2015',
            '05/2015',
            '06/2015',
            '07/2015',
            '08/2015'
        ])
        sparklines_ranking = sparklines_ranking.rename(columns=self.add_ordered_column_name).reset_index()
        ranking = pd.merge(ranking, sparklines_ranking, on=['Label'])
        if 'Objectifs' in ranking.columns:
            ranking.drop(['Objectifs'], axis=1)

        return ranking.drop(['Date'], axis=1)

    @classmethod
    def guard_against_missing_column(cls, column_name, data_frame, source):
        if column_name not in data_frame.columns:
            raise Exception('Columnn "{}" is missing from "{}" data frame.'.format(column_name, source))
        else:
            return True

    @staticmethod
    def add_ordered_column_name(column):
        parts = column.split('/')
        month = parts[0]
        year = parts[1]
        return 'sparkline-' + year + '/' + month

    @classmethod
    def remove_unavailable_metrics(cls, df):
        for month in range(cls.TIME_SERIES_END_MONTH + 1, 12):
            df = df[df['Date'] != "{0:02d}".format(month) + '/2015']

        return df[df['Date'] != '12/2015']

    @classmethod
    def keep_first_semester_metrics(cls, df):
        """
        Remove 2014 metrics in order to keep 2015 first semester metrics only

        :param df:
        :return:
        """
        df = cls.remove_unavailable_metrics(df)
        for month in range(1, 13):
            df = df[df['Date'] != "{0:02d}".format(month) + '/2014']

        return df

    @staticmethod
    def remove_companies_rows(df):
        df = df[df['Label'] != 'TOTAL France']
        df = df[df['Label'] != 'OUEST ']
        df = df[df['Label'] != 'NORD EST']
        df = df[df['Label'] != 'SUD ']
        df = df[df['Label'] != 'Ouest']
        df = df[df['Label'] != 'Nord-Est']
        return df[df['Label'] != 'Sud']

    def preprocess_target_metrics(self):
        """
        Remove France turnover values

        :return:
        """

        target_metrics = self.pack_growth_frame('turnover', exclude_country=False)
        target_metrics.replace({'Label': {'TOTAL France': 'France'}}, inplace=True)
        country_target_metrics = target_metrics[target_metrics['Label'] == 'France'].copy().replace(
            {'Pack': {'Entreprises': 'Pays'}})

        target_metrics = target_metrics[target_metrics['Label'] != 'France']
        target_metrics = pd.concat([country_target_metrics, target_metrics])

        target_label = u'Objectifs ' + self.LATEST_MONTH + ' 2015'
        turnover_label = self.LATEST_MONTH + u' 2015'
        target_metrics = target_metrics.rename(columns={
            'Objectifs Mois': target_label,
            'Chiffre d\'affaires': turnover_label,
            'Prog n-1 Mois': 'progression'
        })
        target_metrics['progression'] = 100*(target_metrics[turnover_label] - target_metrics[target_label]) / target_metrics[target_label]
        target_metrics['progression'] = target_metrics['progression'].apply(lambda growth: '%.2f' % round(growth, 2))
        target_metrics[turnover_label] /= 1000
        target_metrics[target_label] /= 1000

        self.preprocessed_data_frames['target_metrics'] = target_metrics

    def preprocess_latest_metrics(self):
        self.preprocess_latest_metrics_frames({
            'turnover_progress': {
                'rename_columns': {'Turnover': 'Value'},
                'breakdown': 'Chiffre d\'affaires'
            },
            'article_progress': {
                'rename_columns': {'Articles': 'Value'},
                'breakdown': 'Articles'
            },
            'customer_progress': {
                'rename_columns': {'Clients': 'Value'},
                'breakdown': 'Clients'
            },
            'customer_article_progress': {
                'rename_columns': {'Articles par client': 'Value'},
                'breakdown': 'Articles par client'
            },
            'average_basket_progress': {
                'rename_columns': {'Panier moyen': 'Value'},
                'breakdown': 'Panier moyen'
            },
            #'average_price_progress': {
                #'rename_columns': {'Prix de vente moyen': 'Value'},
                #'breakdown': 'Prix de vente moyen'
            #}
        })
        self.latest_metrics[0]['Value'] = self.latest_metrics[0]['Value'] / 1000000
        self.latest_metrics[1]['Value'] = self.latest_metrics[1]['Value'] / 1000
        self.latest_metrics[2]['Value'] = self.latest_metrics[2]['Value'] / 1000000
        #self.latest_metrics[0]['Value'] = self.latest_metrics[0]['Value'] / 1000
        #self.latest_metrics[3]['Value'] = self.latest_metrics[3]['Value'] / 1000000
        #self.latest_metrics[2]['Value'] = self.latest_metrics[2]['Value'] / 1000000

        self.preprocessed_data_frames['latest_metrics'] = pd.concat(self.latest_metrics)

    def preprocess_latest_metrics_frames(self, frames):
        for source, properties in frames.iteritems():
            self.preprocess_latest_metrics_frame(source, properties['rename_columns'], properties['breakdown'])

    def preprocess_latest_metrics_frame(self, source, rename_columns, breakdown):
        latest_metrics = self.pack_growth_frame(source)
        latest_metrics = latest_metrics.rename(columns=rename_columns)
        if 'Prog N-1' in latest_metrics.columns:
            latest_metrics = latest_metrics.drop(['Prog N-1'], axis=1)
        if 'Objectifs' in latest_metrics.columns:
            latest_metrics = latest_metrics.drop(['Objectifs'], axis=1)
        latest_metrics = latest_metrics.drop(['Date', 'parent_zone'], axis=1)
        latest_metrics.loc[:, 'Breakdown'] = breakdown
        self.latest_metrics.append(latest_metrics)

    def get_latest_data_availability_date(self):
        return self.LATEST_DATA_AVAILABILITY_DATE

    def preprocess_growths(self):
        self.preprocess_growth_frames({
            'turnover_progress': {
                'drop_columns': ['Turnover', 'Objectifs', 'Date', 'parent_zone'],
                'breakdown': 'Chiffre d\'affaires'
            },
            #'customer_article_progress': {
                #'drop_columns': ['Articles par client', 'Date', 'parent_zone'],
                #'breakdown': 'Articles par client'
            #},
            'article_progress': {
                'drop_columns': ['Articles', 'Date', 'parent_zone'],
                'breakdown': 'Articles'
            },
            'customer_progress': {
                'drop_columns': ['Clients', 'Date', 'parent_zone'],
                'breakdown': 'Clients'
            },
            #'average_basket_progress': {
                #'drop_columns': ['Panier moyen', 'Date', 'parent_zone'],
                #'breakdown': 'Panier moyen'
            #},
            #'average_price_progress': {
                #'drop_columns': ['Prix de vente moyen', 'Date', 'parent_zone'],
                #'breakdown': 'Prix de vente moyen'
            #}
        })

        self.preprocessed_data_frames['growths'] = pd.concat(self.growths)
        self.preprocessed_data_frames['growths']['Value'] = 3.1*self.preprocessed_data_frames['growths']['Value']

    def preprocess_growth_frames(self, frames):
        for source, properties in frames.iteritems():
            growth = self.preprocess_growth_frame(source)

            for column in properties['drop_columns']:
                self.guard_against_missing_column(column, growth, source)

            growth = growth.drop(properties['drop_columns'], axis=1)
            growth.loc[:, 'Breakdown'] = properties['breakdown']
            self.growths.append(growth)

    def preprocess_growth_frame(self, source):
        growth = self.pack_growth_frame(source)
        return growth.rename(columns={'Prog N-1': 'Value', 'Prog n-1': 'Value'})

    def pack_growth_frame(self, source, exclude_country=True):
        """
        Set Pack before removing data older than our latest data availability date
        and rename labels after removing France rows

        :param source:
        :param exclude_country:
        :return:
        """
        growth = self.data_frames[source]

        shared_substitutions = self.get_shared_substitutions()
        if source in shared_substitutions:
            growth = growth.rename(columns=shared_substitutions[source])

        growth.loc[:, 'Pack'] = self.get_parent_zone_column(source=source)
        growth = growth[growth['Date'] == self.get_latest_data_availability_date()]
        if exclude_country:
            growth = growth[growth['Label'] != 'TOTAL France']
        return growth.replace({'Label': {'NORD EST': 'Nord-Est', 'SUD ': 'Sud', 'OUEST ': 'Ouest'}})

    def preprocess_turnover(self):
        self.turnover_progress = self.data_frames['turnover_progress']
        self.turnover_progress = self.turnover_progress.drop('Objectifs', axis=1)
        self.turnover_progress = self.turnover_progress.rename(columns={'Valeur': 'Value'})
        self.turnover_progress.loc[:, 'parent_zone'] = self.get_parent_zone_column('turnover_progress')

        turnover_progress = self.turnover_progress
        border = self.preprocess_waterfall_border(turnover_progress)

        waterfall_cumulated_values = border.cumsum()

        start_date = self.EARLIEST_DATA_AVAILABILITY_DATE

        start_index = border[border['Date'] == start_date].index
        start = border[border['Date'] == start_date].copy()
        start.ix[start_index, 'Value'] = waterfall_cumulated_values.ix[start_index]['Value']
        start.loc[:, 'Label'] = start['Label'].apply(lambda label: 'CUMUL janvier-' + self.LATEST_MONTH + ' 2014')
        start = start.drop('Date', axis=1)

        end_index = border[border['Date'] == self.get_latest_data_availability_date()].index
        end = border[border['Date'] == self.get_latest_data_availability_date()].copy()

        end_semester_index = border[border['Date'] == '12/2014'].index
        end_semester_extra = waterfall_cumulated_values.ix[end_semester_index, 'Value'].values
        end.ix[end_index, 'Value'] = waterfall_cumulated_values.ix[end_index]['Value'].values - end_semester_extra

        end.loc[:, 'Label'] = end['Label'].apply(lambda label: 'CUMUL janvier-' + self.LATEST_MONTH + ' 2015 ')
        end = end.drop('Date', axis=1)

        north_eastern_cumulated_turnover = self.preprocess_turnover_territory('Nord-Est', 'Nord-Est')
        north_eastern_territories = self.compute_territories_turnover_differences(north_eastern_cumulated_turnover)

        southern_cumulated_territories = self.preprocess_turnover_territory('Sud', 'Sud')
        southern_territories = self.compute_territories_turnover_differences(southern_cumulated_territories)

        western_cumulated_territories = self.preprocess_turnover_territory('Ouest', 'Ouest')
        western_territories = self.compute_territories_turnover_differences(western_cumulated_territories)

        all_territories = pd.concat([
            north_eastern_cumulated_turnover,
            southern_cumulated_territories,
            western_cumulated_territories
        ])

        # Override provided data which are inconsistent with territories decomposition
        start.ix[start_index, 'Value'] = all_territories['Value'].sum()
        end.ix[end_index, 'Value'] = all_territories['End Value'].sum()

        turnover_frame = pd.concat([start, north_eastern_territories, southern_territories, western_territories, end])
        turnover_frame.loc[:, 'Value'] = turnover_frame['Value'] / 1000000

        self.preprocessed_data_frames['turnover_waterfall'] = turnover_frame

    def preprocess_turnover_territory(self, parent_zone, label):
        territories = self.turnover_progress[self.turnover_progress['parent_zone'] == parent_zone]
        territories = territories.rename(columns={'Label': 'Sublevel Label'})
        territories.loc[:, 'Label'] = territories['parent_zone'].apply(lambda territory_label: label)

        companies = self.turnover_progress[self.turnover_progress['parent_zone'] == parent_zone]['Label'].unique()

        for company in companies:
            territories_company = territories[territories['Sublevel Label'] == company]

            company_cumulated_values = territories_company.cumsum()

            # Store cumulated total of turnover for a given company (Sublevel Label)
            # in a given territory (parent_zone)
            # in August 2014
            keep_company_start_date = territories_company['Date'] == self.EARLIEST_DATA_AVAILABILITY_DATE
            start_date_index = territories_company[keep_company_start_date].index
            territories.ix[start_date_index, 'Value'] = company_cumulated_values.ix[start_date_index]['Value'].values

            # Store cumulated total of turnover for a given company (Sublevel Label)
            # in a given territory (parent_zone)
            # in August 2015
            keep_company_end_date = territories_company['Date'] == self.get_latest_data_availability_date()
            end_date_index = territories_company[keep_company_end_date].index
            territories.ix[end_date_index, 'Value'] = company_cumulated_values.ix[end_date_index]['Value']

            # Substract cumulated total of turnover for a given company (Sublevel Label)
            # in a given territory (parent_zone)
            # in December 2014 from cumulated total of turnover in August 2015
            last_semester_end = territories_company['Date'] == '12/2014'
            last_semester_end_index = territories_company[last_semester_end].index
            last_year_extra = company_cumulated_values.ix[last_semester_end_index]['Value'].values
            territories.ix[end_date_index, 'Value'] = territories.ix[end_date_index, 'Value'].values - last_year_extra

        end_territories = territories[territories['Date'] == self.get_latest_data_availability_date()]
        end_territories = end_territories.rename(columns={'Value': 'End Value'})

        start_territories = territories[territories['Date'] == self.EARLIEST_DATA_AVAILABILITY_DATE]

        start_territories = start_territories.drop(['Prog N-1', 'parent_zone', 'Date'], axis=1)
        end_territories = end_territories.drop(['Prog N-1', 'parent_zone', 'Date'], axis=1)

        return pd.merge(start_territories, end_territories, on=['Label', 'Sublevel Label'])

    @classmethod
    def compute_territories_turnover_differences(cls, territories):
        territories_differences = territories.copy()
        territories_differences.loc[:, 'Value'] = territories['End Value'] - territories['Value']
        return territories_differences.drop(['End Value'], axis=1)

    @staticmethod
    def preprocess_waterfall_border(progresses):
        border = progresses[progresses['Label'] == 'TOTAL France'].copy()
        border.loc[:, 'Sublevel Label'] = ''
        border = border.drop(['Label', 'Prog N-1'], axis=1)
        return border.rename(columns={
            'parent_zone': 'Label'
        })

    def preprocess_monthly_metrics(self):
        progress_metrics = self.get_progresses(monthly=True)
        progress_metrics = progress_metrics[progress_metrics['Label'].str.contains('TOTAL France') == True]
        drop_columns = [
            'parent_zone',
            'Articles',
            'Articles par client',
            'Clients',
            'Label',
            'Turnover',
            'Panier moyen',
            #'Prix de vente moyen',
        ]
        progress_metrics = progress_metrics.drop(drop_columns, axis=1)
        progress_metrics = progress_metrics[progress_metrics['Date'].map(self.keep_last_year_metrics)]
        self.data_frames['progress_metrics'] = progress_metrics
        self.rename_data_frames_columns({
            'progress_metrics': {
                'turnover n-1': 'Chiffre d\'affaires',
                'customer n-1': 'Clients',
                'article n-1': 'Articles',
                'customer article n-1': 'Articles par client',
                #'average price n-1': 'Prix de vente moyen'
            }
        })
        progress_metrics = pd.melt(self.preprocessed_data_frames['progress_metrics'],
                                   value_vars=[
                                       'Chiffre d\'affaires',
                                       'Clients',
                                       'Articles',
                                       'Articles par client',
                                       #'Prix de vente moyen'
                                       ],
                                   id_vars=['Date'],
                                   value_name='Value',
                                   var_name='Label')

        self.preprocessed_data_frames['progress_metrics'] = progress_metrics

    def keep_last_year_metrics(self, date):
        date_parts = date.split('/')
        month = date_parts[0]
        year = date_parts[1]

        if int(year) == self.TIME_SERIES_START_YEAR and int(month) >= self.TIME_SERIES_START_MONTH:
            return True

        if int(year) > self.TIME_SERIES_END_YEAR:
            return False

        if int(year) == self.TIME_SERIES_END_YEAR and int(month) <= self.TIME_SERIES_END_MONTH:
            return True

        return False

    def preprocess_dashboards(self):
        """
        Preprocess KPIs rendered on dashboards

        :return: void
        """
        merge_data_frames = self.get_progresses()

        data_frames = self.keep_first_semester_metrics(merge_data_frames)
        cumulated_values = data_frames.groupby(['Label']).sum().reset_index()
        clients = data_frames[['Label','Clients','Date']]
        cc = clients[ clients['Date']== self.LATEST_DATA_AVAILABILITY_DATE ]
        cumulated_values['Clients'] = cc.sort('Label').reset_index()['Clients']
        articles = data_frames[['Label','Articles','Date']]
        aa = articles[ articles['Date']== self.LATEST_DATA_AVAILABILITY_DATE ]
        cumulated_values['Articles'] = aa.sort('Label').reset_index()['Articles']
        cumulated_values = cumulated_values.drop([
            'article n-1', 'turnover n-1', 'average price n-1', 'average basket n-1',
            'customer n-1', 'customer article n-1',
            'Prix de vente moyen', 'Panier moyen', 'Objectifs',
            'Marge nette cumul'
        ], axis=1)

        merge_data_frames['Monthly Turnover'] = merge_data_frames['Turnover']

        for label in cumulated_values['Label'].unique():
            cumulated_df = merge_data_frames[merge_data_frames['Label'] == label]
            index = cumulated_df[cumulated_df['Date'] == self.LATEST_DATA_AVAILABILITY_DATE].index

            labeled_turnover = cumulated_values[cumulated_values['Label'] == label]['Turnover']
            merge_data_frames.ix[index, 'Turnover'] = labeled_turnover.values

            labeled_customers = cumulated_values[cumulated_values['Label'] == label]['Clients']
            merge_data_frames.ix[index, 'Clients'] = labeled_customers.values

            labeled_articles = cumulated_values[cumulated_values['Label'] == label]['Articles']
            merge_data_frames.ix[index, 'Articles'] = labeled_articles.values

        divisor = 1000000
        merge_data_frames['Articles'] /= divisor
        merge_data_frames['Clients'] /= divisor
        merge_data_frames['Monthly Turnover'] /= divisor
        merge_data_frames['monthly_turnover'] /= divisor
        merge_data_frames['cumulated_disbursed_fees'] /= divisor
        merge_data_frames['cumulated_net_margin'] /= divisor
        merge_data_frames['cumulated_turnover'] /= divisor
        merge_data_frames['cumulated_raw_cash_flow'] /= divisor
        merge_data_frames['cumulated_net_cash_flow'] /= divisor
        merge_data_frames['cumulated_distance_to_target'] /= divisor
        merge_data_frames['monthly_distance_to_target'] /= divisor

        merge_data_frames['default'] = None

        # Copy these columns so that they can be exposed to Jinja template
        # not allowing whitespace and dashes
        copy_columns = {
            'Articles':             'article',
            'article n-1':          'article_progress',
            'Panier moyen':         'average_basket',
            'average basket n-1':   'average_basket_progress',
            #'Prix de vente moyen':  'average_price',
            #'average price n-1':    'average_price_progress',
            'Clients':              'customer',
            'customer n-1':         'customer_progress',
            'Articles par client':  'customer_article',
            'customer article n-1': 'customer_article_progress'
        }

        for source, destination in copy_columns.iteritems():
            merge_data_frames[destination] = merge_data_frames[source]

        default_report_index = merge_data_frames[merge_data_frames['Label'] == 'TOTAL France'].index
        merge_data_frames.ix[default_report_index, 'default'] = True

        self.preprocessed_data_frames['progresses'] = merge_data_frames

    def get_progresses(self, monthly=False):
        self.preprocessed_data_frames['customer_article_progress'] = self.data_frames['customer_article_progress']
        self.preprocessed_data_frames['customer_article_progress'].loc[:, 'parent_zone'] = self.get_parent_zone_column()
        rename_columns = {
            'customer_article_progress': {
                'Prog n-1': 'customer article n-1',
            },
            'article_progress': {
                'Prog n-1': 'article n-1',
            },
            'turnover_progress': {
                'Prog N-1': 'turnover n-1'
            },
            'customer_progress': {
                'Prog n-1': 'customer n-1'
            },
            #'average_price_progress': {
                #'Prog n-1': 'average price n-1',
            #},
            'average_basket_progress': {
                'Prog n-1': 'average basket n-1'
            }
        }

        merge_columns = {
            'article_progress': ['Date', 'Label'],
            'turnover_progress': ['Date', 'Label'],
            'customer_progress': ['Date', 'Label'],
            #'average_price_progress': ['Date', 'Label'],
            'average_basket_progress': ['Date', 'Label']
        }

        if not monthly:
            rename_columns['turnover'] = {
                'Chiffre d\'affaires': 'monthly_turnover',
                'Chiffre d\'affaires cumul': 'cumulated_turnover',
                'Prog n-1 Cumul': 'cumulated_turnover_progress',
                'Prog n-1 Mois': 'monthly_turnover_progress',
                'Ecart sur Objectif Mois': 'monthly_distance_to_target',
                'Ecart sur Objectif Cumul': 'cumulated_distance_to_target',
            }
            rename_columns['net_margins'] = {
                'Marge nette': 'Net margin',
                'Marge nette cumul': 'cumulated_net_margin',
                'Prog n-1 Cumul': 'cumulated_net_margin_progress'
            }
            rename_columns['disbursed_fees'] = {
                u'Total des frais décaissés cumul': 'cumulated_disbursed_fees',
                'Prog n-1 Cumul': 'cumulated_disbursed_fees_progress',
            }
            rename_columns['raw_cash_flow'] = {
                u'Cash Flow Brut cumul': 'cumulated_raw_cash_flow',
                'Prog n-1 Cumul': 'cumulated_raw_cash_flow_progress'
            }
            rename_columns['net_cash_flow'] = {
                'Participation Cumul': 'cumulated_net_cash_flow',
                'Prog n-1 Cumul': 'cumulated_net_cash_flow_progress'
            }

            merge_columns['disbursed_fees'] = ['Date', 'Label']
            merge_columns['net_margins'] = ['Date', 'Label']
            merge_columns['turnover'] = ['Date', 'Label']
            merge_columns['raw_cash_flow'] = ['Date', 'Label']
            merge_columns['net_cash_flow'] = ['Date', 'Label']

        self.rename_data_frames_columns(rename_columns)
        self.rename_data_frames_columns(self.get_shared_substitutions(), self.preprocessed_data_frames)

        reversed_data_frames = self.reverse_data_frame(self.preprocessed_data_frames['customer_article_progress'])

        return self.merge_data_frames(reversed_data_frames, merge_columns)

    def rename_data_frames_columns(self, rename_columns, source_dfs=None, destination_dfs=None):
        """
        Rename columns in source data frames
        before storing transformed data frames in preprocessed data frames

        :param rename_columns:
        :return:
        """
        if source_dfs is None:
            source_dfs = self.data_frames

        if destination_dfs is None:
            destination_dfs = self.preprocessed_data_frames

        for data_frame_name, columns in rename_columns.iteritems():
            if data_frame_name in source_dfs:
                destination_dfs[data_frame_name] = source_dfs[data_frame_name].rename(columns=columns)

    @staticmethod
    def get_shared_substitutions():
        """
        Columns to be renamed similarly for distinct data frames to be preprocessed

        :return: dict
        """
        return {
            'customer_article_progress': {
                'Articles': 'Articles par client'
            },
            'article_progress': {
                'Clients': 'Articles',
            },
            'turnover_progress': {
                'Valeur': 'Turnover'
            },
            #'average_price_progress': {
                #'Articles par client': 'Prix de vente moyen'
            #},
            'net_margins': {
                'Valeur': 'Net margin',
            },
            'disbursed_fees': {
                'Valeur': 'Disbursed Fees',
            }
        }

    @staticmethod
    def reverse_data_frame(data_frame):
        return data_frame[::-1]

    def get_parent_zone_column(self, source='customer_article_progress'):
        progress_frame = self.data_frames[source]
        progress_frame.loc[:, 'parent_zone'] = progress_frame['Label']
        return progress_frame['parent_zone'].apply(self.fill_cell_with_parent_zone)

    def fill_cell_with_parent_zone(self, zone):
        if zone in ['Nord-Est', 'Ouest', 'Sud', 'TOTAL France']:

            if zone == 'Nord-Est':
                self.current_parent_zone = 'Nord-Est'
            else:
                self.current_parent_zone = zone.capitalize().strip()

            return 'Entreprises'

        return self.current_parent_zone

    def merge_data_frames(self, left_data_frame, merge_columns):
        for data_frame_name, on_columns in merge_columns.iteritems():
            left_data_frame = pd.merge(left_data_frame, self.preprocessed_data_frames[data_frame_name],
                                       how='outer',
                                       on=on_columns)
        return left_data_frame

    def get_preprocessed_data_frames(self):
        return self.preprocessed_data_frames
