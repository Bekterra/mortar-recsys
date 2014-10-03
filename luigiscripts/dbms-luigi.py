import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import dbms
from mortar.luigi import mortartask


"""
This luigi pipeline runs the Last.fm example, pulling data from S3 and putting the
results in a DBMS.  Luigi tracks progress by writing intermediate data to S3.

To run, set up client.cfg with your Mortar username and API key, your s3 keys and your dbms
connction string.
Task Order:
    GenerateSignals
    ItemItemRecs
    UserItemRecs
    CreateIITable
    CreateUITable
    WriteDBMSTables
    SanityTestIITable
    SanityTestUITable
    ShutdownClusters

To run:
    mortar luigi luigiscripts/dbms-luigi.py \
        --output-base-path "s3://<your-bucket>/dbms-lastfm" \
        --data-store-path "s3://<your-bucket>/lastfm-data" \
        --table-name-prefix "<table-name-prefix>"
"""

# helper function
def create_full_path(base_path, sub_path):
    return '%s/%s' % (base_path, sub_path)

# REPLACE WITH YOUR PROJECT NAME
MORTAR_PROJECT = '<your-project-name>'

# location of Last.FM data in S3
LAST_FM_INPUT_SIGNALS_PATH = \
    's3://mortar-example-data/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv'

class LastfmPigscriptTask(mortartask.MortarProjectPigscriptTask):
    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    data_store_path = luigi.Parameter()

    # cluster size to use
    cluster_size = luigi.IntParameter(default=10)

    pig_version = '0.12'

    def project(self):
        """
        Name of the mortar project to run.
        """
        return MORTAR_PROJECT

    def token_path(self):
        return self.output_base_path

    def default_parallel(self):
        return (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE

class GenerateSignals(LastfmPigscriptTask):
    """
    Runs the 01-dbms-generate-signals.pig Pigscript
    """

    def requires(self):
        return [S3PathTask(LAST_FM_INPUT_SIGNALS_PATH)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user_signals'))]

    def parameters(self):
        return {'INPUT_SIGNALS': LAST_FM_INPUT_SIGNALS_PATH,
                'OUTPUT_PATH': self.output_base_path}

    def script(self):
        """
        Name of the script to run.
        """
        return 'dbms/01-dbms-generate-signals'


class ItemItemRecs(LastfmPigscriptTask):
    """
    Runs the 02-dbms-item-item-recs.pig Pigscript
    """

    def requires(self):
        return [GenerateSignals(output_base_path=self.output_base_path, data_store_path=self.data_store_path)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'item_item_recs'))]

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'LOGISTIC_PARAM': 2.0,
                'MIN_LINK_WEIGHT': 1.0,
                'MAX_LINKS_PER_USER': 100,
                'BAYESIAN_PRIOR': 4.0,
                'NUM_RECS_PER_ITEM': 5,
                'default_parallel': self.default_parallel()}

    def script(self):
        """
        Name of the script to run.
        """
        return 'dbms/02-dbms-item-item-recs'

class UserItemRecs(LastfmPigscriptTask):
    """
    Runs the 03-dbms-user-item-recs.pig Pigscript
    """

    def requires(self):
        return [ItemItemRecs(output_base_path=self.output_base_path, data_store_path=self.data_store_path)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user_item_recs'))]

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'NUM_RECS_PER_USER': 5,
                'ADD_DIVERSITY_FACTOR':False,
                'default_parallel': self.default_parallel()}

    def script(self):
        """
        Name of the script to run.
        """
        return 'dbms/03-dbms-user-item-recs'


class CreateIITable(dbms.CreatePostgresTable):
    """
    Creates a DynamoDB table for storing the item-item recommendations
    """

    # Unused: passing parameter
    data_store_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    table_name_prefix = luigi.Parameter()

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def primary_key(self):
        return ['from_id', 'rank']

    def field_string(self):
        return 'from_id varchar, to_id varchar, weight decimal, raw_weight decimal, rank int'

    # append 'ii' to distinguish between this and the user-item table
    def table_name(self):
        return '%s%s' % (self.table_name_prefix, 'ii')

    def requires(self):
        return [UserItemRecs(output_base_path=self.output_base_path, data_store_path=self.data_store_path)]

class CreateUITable(dbms.CreatePostgresTable):
    """
    Creates a Postgres table for storing the user-item recommendations
    """

    # Unused: passing parameter
    data_store_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    table_name_prefix = luigi.Parameter()

    def primary_key(self):
        return ['from_id', 'rank']

    def field_string(self):
        return 'from_id varchar, to_id varchar, weight decimal, reason_item varchar, user_reason_item_weight decimal, item_reason_item_weight decimal, rank int'

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # append 'ui' to distinguish between this and the item-item table
    def table_name(self):
        return '%s%s' % (self.table_name_prefix, 'ui')

    def requires(self):
        return [UserItemRecs(output_base_path=self.output_base_path, data_store_path=self.data_store_path)]


class WriteDBMSTables(LastfmPigscriptTask):
    """
    Runs the 04-write-results-to-dbms.pig Pigscript
    """

    # Unused: passing parameter
    data_store_path = luigi.Parameter()

    # root name of the table
    table_name_prefix = luigi.Parameter()

    def requires(self):
        return [CreateUITable(output_base_path=self.output_base_path, table_name_prefix=self.table_name_prefix, data_store_path=self.data_store_path),
                CreateIITable(output_base_path=self.output_base_path, table_name_prefix=self.table_name_prefix, data_store_path=self.data_store_path)]

    def script_output(self):
        return []

    def parameters(self):
        return {'DATABASE_DRIVER': 'org.postgresql.Driver',
                'DATABASE_TYPE': 'postgresql',
                'DATABASE_HOST': '%s:%s' % (configuration.get_config().get('postgres', 'host'), configuration.get_config().get('postgres', 'port')),
                'DATABASE_NAME': configuration.get_config().get('postgres', 'dbname'),
                'DATABASE_USER': configuration.get_config().get('postgres', 'user'),
                'II_TABLE': '%s%s' % (self.table_name_prefix, 'ii'),
                'UI_TABLE': '%s%s' % (self.table_name_prefix, 'ui'),
                'OUTPUT_PATH': self.output_base_path
               }

    def script(self):
        """
        Name of the script to run.
        """
        return 'dbms/04-write-results-to-dbms'

class SanityTestIITable(dbms.SanityTestPostgresTable):
    """
    Check that the database contains expected data
    """

    #Id field to check
    def id_field(self):
        return 'from_id'

    # Unused: passing parameter
    data_store_path = luigi.Parameter()

    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # name of the collection
    table_name_prefix = luigi.Parameter()

    # append 'ii' to distinguish between this and the item-item collection
    def table_name(self):
        return '%s%s' % (self.table_name_prefix, 'ii')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # sentinel ids expected to be in the result data
    def ids(self):
        return ["the beatles", "miley cyrus", "yo-yo ma", "ac dc", "coldplay"]

    def requires(self):
        return [WriteDBMSTables(output_base_path=self.output_base_path,
                                        table_name_prefix=self.table_name_prefix,
                                        data_store_path=self.data_store_path)]

class SanityTestUITable(dbms.SanityTestPostgresTable):
    """
    Check that the database contains expected data
    """

    #Id field to check
    def id_field(self):
        return 'from_id'

    # Unused: passing parameter
    data_store_path = luigi.Parameter()

    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # name of the collection
    table_name_prefix = luigi.Parameter()

    # append 'ui' to distinguish between this and the item-item collection
    def table_name(self):
        return '%s%s' % (self.table_name_prefix, 'ui')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # sentinel ids expected to be in the result data
    def ids(self):
        return ["faf0805d215993c5ff261e58a5358131cf2b2a60", "faf0aa22d8621be9ed7222e3867caf1a560d8785", "faf0c313b1952ba6d83f390dedea81379eed881a", "faf12b4c90e90cb77adc284f0a5970decad86bde", "faf18c1cca1a4172011334821e0c124a7eedfa50"]

    def requires(self):
        return [SanityTestIITable(output_base_path=self.output_base_path,
                                       table_name_prefix=self.table_name_prefix,
                                       data_store_path=self.data_store_path)]

class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    When the pipeline is completed, shut down all active clusters not currently running jobs
    """

    # Unused: passing parameter
    data_store_path = luigi.Parameter()

    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # unused, but must be passed through
    table_name_prefix = luigi.Parameter()

    def requires(self):
        return [SanityTestUITable(output_base_path=self.output_base_path,
                                       table_name_prefix=self.table_name_prefix,
                                       data_store_path=self.data_store_path)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]

if __name__ == "__main__":
    luigi.run(main_task_cls=ShutdownClusters)
