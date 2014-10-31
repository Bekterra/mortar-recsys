import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import dbms
from mortar.luigi import mortartask


"""
This luigi pipeline runs the Last.fm example, pulling data from S3 and putting the
results in a DBMS.  Luigi tracks progress by writing intermediate data to S3.


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
        --table-name-prefix "<table-name-prefix>"
"""

def create_full_path(base_path, sub_path):
    """
    Helper function for constructing paths.
    """
    return '%s/%s' % (base_path, sub_path)

# The location of the Last.FM data in S3.
LAST_FM_INPUT_SIGNALS_PATH = \
    's3://mortar-example-data/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv'


class LastfmPigscriptTask(mortartask.MortarProjectPigscriptTask):
    """
    This is the base class for all of our Mortar related Luigi Tasks.  It extends
    the generic MortarProjectPigscriptTask to set common defaults we'll use
    for this pipeline: common data paths and default cluster size. 
    """

    # The base path to where output data will be written.  This will be an S3 path.
    output_base_path = luigi.Parameter()

    # The cluster size to use for running Mortar jobs.  A cluster size of 0
    # will run in Mortar's local mode.  This is a fast (and free!) way to run jobs
    # on small data samples.  Cluster sizes >= 2 will run on a Hadoop cluster.
    cluster_size = luigi.IntParameter(default=10)

    def token_path(self):
        """
        Luigi manages dependencies between tasks by checking for the existence of
        files.  When one task finishes it writes out a 'token' file that will
        trigger the next task in the dependency graph.  This is the base path for
        where those tokens will be written.
        """
        return self.output_base_path

    def default_parallel(self):
        """
        This is used for an optimization that tells Hadoop how many reduce tasks should be used
        for a Hadoop job.  By default we'll tell Hadoop to use the number of reduce slots
        in the cluster.
        """
        if self.cluster_size - 1 > 0:
            return (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE
        else:
            return 1


class GenerateSignals(LastfmPigscriptTask):
    """
    This task runs the 01-dbms-generate-signals.pig pigscript.
    """

    def requires(self):
        """
        The requires method is how you build your dependency graph in Luigi.  Luigi will not
        run this task until all tasks returning in this list are complete.

        S3PathTask is a simple Luigi task that ensures some path already exists. As
        GenerateSignals is the first task in this Luigi pipeline it only requires that its
        expected input data can be found.
        """
        return [S3PathTask(LAST_FM_INPUT_SIGNALS_PATH)]

    def script_output(self):
        """
        The script_output method is how you define where the output from this task will be stored.

        Luigi will check this output location before starting any tasks that depend on this task.
        """
        return [S3Target(create_full_path(self.output_base_path, 'user_signals'))]

    def parameters(self):
        """
        This method defines the parameters that will be passed to Mortar when starting
        this pigscript.
        """
        return {'INPUT_SIGNALS': LAST_FM_INPUT_SIGNALS_PATH,
                'OUTPUT_PATH': self.output_base_path}

    def script(self):
        """
        This is the name of the pigscript that will be run.

        You can find this script in the pigscripts directory of your Mortar project.
        """
        return 'dbms/01-dbms-generate-signals'


class ItemItemRecs(LastfmPigscriptTask):
    """
    This task runs the 02-dbms-item-item-recs.pig pigscript.
    """

    def requires(self):
        """
        Tell Luigi to run the GenerateSignals task before this task.
        """
        return [GenerateSignals(output_base_path=self.output_base_path)]

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
        return 'dbms/02-dbms-item-item-recs'


class UserItemRecs(LastfmPigscriptTask):
    """
    This task runs the 03-dbms-user-item-recs.pig pigscript
    """

    def requires(self):
        """
        Tell Luigi to run the ItemItemRecs task before this task.
        """
        return [ItemItemRecs(output_base_path=self.output_base_path)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user_item_recs'))]

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'NUM_RECS_PER_USER': 5,
                'ADD_DIVERSITY_FACTOR':False,
                'default_parallel': self.default_parallel()}

    def script(self):
        return 'dbms/03-dbms-user-item-recs'


class CreateIITable(dbms.CreatePostgresTable):
    """
    This task creates a database table for storing the item-item recommendations.
    """

    # As this task is creating a database table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The base name of the table to be created.
    table_name_prefix = luigi.Parameter()

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def requires(self):
        """
        Tell Luigi that the UserItemRecs task needs to be completed
        before running this task.
        """
        return [UserItemRecs(output_base_path=self.output_base_path)]

    """
    Methods returning information about the table to be created.
    """

    def primary_key(self):
        return ['from_id', 'rank']

    def field_string(self):
        return 'from_id varchar, to_id varchar, weight decimal, raw_weight decimal, rank int'

    def table_name(self):
        # Append '-II' to distinguish between this table and the user-item table.
        return '%s%s' % (self.table_name_prefix, 'ii')


class CreateUITable(dbms.CreatePostgresTable):
    """
    This task creates a database table for storing the user-item recommendations.
    """

    # As this task is creating a database table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The base name of the table to be created.
    table_name_prefix = luigi.Parameter()

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def requires(self):
        """
        Tell Luigi that the UserItemRecs task needs to be completed
        before running this task.
        """
        return [UserItemRecs(output_base_path=self.output_base_path)]

    """
    Methods returning information about the table to be created.
    """

    def primary_key(self):
        return ['from_id', 'rank']

    def field_string(self):
        return 'from_id varchar, to_id varchar, weight decimal, reason_item varchar, user_reason_item_weight decimal, item_reason_item_weight decimal, rank int'

    def table_name(self):
        # Append '-UI' to distinguish between this table and the item-item table.
        return '%s%s' % (self.table_name_prefix, 'ui')


class WriteDBMSTables(LastfmPigscriptTask):
    """
    This task runs the 04-write-results-to-dbms.pig pigscript which writes the 
    recommendations to a database.
    """

    # The base name of the tables to be written to.
    table_name_prefix = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that both the CreateUITable and CreateIITable tasks need to be completed
        before running this task.
        """
        return [CreateUITable(output_base_path=self.output_base_path, 
                              table_name_prefix=self.table_name_prefix),
                CreateIITable(output_base_path=self.output_base_path, 
                              table_name_prefix=self.table_name_prefix)]

    def script_output(self):
        """
        Because the recommendations are being written directly to the database there is no
        S3 output for this task.
        """
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
        return 'dbms/04-write-results-to-dbms'


class SanityTestIITable(dbms.SanityTestPostgresTable):
    """
    This task will run a sanity test against our newly created item-item
    table by checking if it contains some expected data.
    """

    #Id field to check.
    def id_field(self):
        return 'from_id'

    # As this task is only reading from a database table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The base name of the recommendation tables.
    table_name_prefix = luigi.Parameter()

    def table_name(self):
        return '%s%s' % (self.table_name_prefix, 'ii')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def ids(self):
        """
        Returns a list of item ids that we expect to be in the result data.
        """
        return ["the beatles", "miley cyrus", "yo-yo ma", "ac dc", "coldplay"]

    def requires(self):
        """
        Tell Luigi that the WriteDBMSTables task needs to be completed
        before running this task.
        """
        return [WriteDBMSTables(output_base_path=self.output_base_path,
                                table_name_prefix=self.table_name_prefix)]


class SanityTestUITable(dbms.SanityTestPostgresTable):
    """
    This task will run a sanity test against our newly created user-item table
    by checking if it contains some expected data.
    """

    #Id field to check.
    def id_field(self):
        return 'from_id'

    # As this task is only reading from a database table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The base name of the recommendation tables.
    table_name_prefix = luigi.Parameter()

    def table_name(self):
        return '%s%s' % (self.table_name_prefix, 'ui')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def ids(self):
        """
        Returns a list of item ids that we expect to be in the result data.
        """
        return ["faf0805d215993c5ff261e58a5358131cf2b2a60", "faf0aa22d8621be9ed7222e3867caf1a560d8785", "faf0c313b1952ba6d83f390dedea81379eed881a", "faf12b4c90e90cb77adc284f0a5970decad86bde", "faf18c1cca1a4172011334821e0c124a7eedfa50"]

    def requires(self):
        """
        Tell Luigi that the SanityTestIITable task needs to be completed
        before running this task.
        """
        return [SanityTestIITable(output_base_path=self.output_base_path,
                                  table_name_prefix=self.table_name_prefix)]


class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    This is the very last task in the pipeline.  It will shut down all active
    clusters that are not currently running jobs.
    """

    # As this task is only shutting down clusters and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # This parameter is unused in this task, but it must be passed up the task dependency graph.
    table_name_prefix = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that the SanityTestUICollection task needs to be completed
        before running this task.
        """
        return [SanityTestUITable(output_base_path=self.output_base_path,
                                  table_name_prefix=self.table_name_prefix)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]

if __name__ == "__main__":
    """
    We tell Luigi to run the last task in the task dependency graph.  Luigi will then
    work backwards to find any tasks with its requirements met and start from there.

    The first time this pipeline is run the only task with its requirements met will be
    GenerateSignals which will find the required input file in S3.
    """
    luigi.run(main_task_cls=ShutdownClusters)
