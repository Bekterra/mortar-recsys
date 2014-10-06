import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask
from boto.dynamodb2.types import NUMBER, STRING

from mortar.luigi import mortartask
from mortar.luigi import dynamodb


"""
This luigi pipeline runs the retail example, pulling data from S3 and storing results in DynamoDB.

To run, replace the value of MORTAR_PROJECT below with your actual project name.
Also, ensure that your AWS keys are correctly configured to write
to DynamoDB (see https://help.mortardata.com/data_apps/recommendation_engine/run_example_pipeline).

Task Order:
    GenerateSignals
    ItemItemRecs
    UserItemRecs
    CreateIITable
    CreateUITable
    WriteDynamoDBTables
    UpdateIITableThroughput
    UpdateUITableThroughput
    SanityTestIITable
    SanityTestUITable
    ShutdownClusters

To run:
    mortar luigi luigiscripts/retail-luigi.py \
      --input-base-path "s3://mortar-example-data/retail-example" \
      --output-base-path "s3://<your-s3-bucket>/retail" \
      --dynamodb-table-name "<dynamo-table-name>"
"""

# REPLACE WITH YOUR PROJECT NAME
MORTAR_PROJECT = 'your-project-name'

def create_full_path(base_path, sub_path):
    """
    Helper function for constructing paths.
    """
    return '%s/%s' % (base_path, sub_path)


class RetailPigscriptTask(mortartask.MortarProjectPigscriptTask):
    """
    This is the base class for all of our Mortar related Luigi Tasks.  It extends
    the generic MortarProjectPigscriptTask to set common defaults we'll use
    for this pipeline: common data paths, default cluster size, and our Mortar project name.
    """

    # The base path to where input data is located.  In most cases your input data
    # should be stored in S3, however for testing purposes you can use a small
    # dataset that is stored in your Mortar project.
    input_base_path = luigi.Parameter()

    # The base path to where output data will be written.  This will be an S3 path.
    output_base_path = luigi.Parameter()

    # The cluster size to use for running Mortar jobs.  A cluster size of 0
    # will run in Mortar's local mode.  This is a fast (and free!) way to run jobs
    # on small data samples.  Cluster sizes >= 2 will run on a Hadoop cluster.
    cluster_size = luigi.IntParameter(default=0)

    def project(self):
        """
        Name of your Mortar project containing this script.
        """
        return MORTAR_PROJECT

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


class GenerateSignals(RetailPigscriptTask):
    """
    This task runs the 01-generate-signals.pig pigscript.
    """

    def requires(self):
        """
        The requires method is how you build your dependency graph in Luigi.  Luigi will not
        run this task until all tasks returning in this list are complete.

        S3PathTask is a simple Luigi task that ensures some path already exists. As
        GenerateSignals is the first task in this Luigi pipeline it only requires that its
        expected input data can be found.
        """
        return [S3PathTask(create_full_path(self.input_base_path, 'purchases.json')),
                S3PathTask(create_full_path(self.input_base_path, 'wishlists.json'))]

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
        return {'INPUT_PATH_PURCHASES': create_full_path(self.input_base_path, 'purchases.json'),
                'INPUT_PATH_WISHLIST': create_full_path(self.input_base_path, 'wishlists.json'),
                'OUTPUT_PATH': self.output_base_path}

    def script(self):
        """
        This is the name of the pigscript that will be run.

        You can find this script in the pigscripts directory of your Mortar project.
        """
        return '01-generate-signals'


class ItemItemRecs(RetailPigscriptTask):
    """
    This task runs the 02-item-item-recs.pig pigscript.
    """

    def requires(self):
        """
        Tell Luigi to run the GenerateSignals task before this task.
        """
        return [GenerateSignals(input_base_path=self.input_base_path,
                                output_base_path=self.output_base_path)]

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
        return '02-item-item-recs'


class UserItemRecs(RetailPigscriptTask):
    """
    This task runs the 03-user-item-recs.pig pigscript
    """

    def requires(self):
        """
        Tell Luigi to run the ItemItemRecs task before this task.
        """
        return [ItemItemRecs(input_base_path=self.input_base_path,
                             output_base_path=self.output_base_path)]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user_item_recs'))]

    def parameters(self):
        return {'OUTPUT_PATH': self.output_base_path,
                'NUM_RECS_PER_USER': 5,
                'ADD_DIVERSITY_FACTOR':False,
                'default_parallel': self.default_parallel()}

    def script(self):
        return '03-user-item-recs'


class CreateIITable(dynamodb.CreateDynamoDBTable):
    """
    This task creates a DynamoDB table for storing the item-item recommendations.
    """

    # The input_base_path used by the RetailPigscriptTasks.
    # This parameter is unused in this task, but it must be passed up the task dependency graph.
    input_base_path = luigi.Parameter()

    # As this task is creating a DynamoDB table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # Parameters required for creating a DynamoDB table
    dynamodb_table_name = luigi.Parameter()
    read_throughput = luigi.IntParameter(1)
    write_throughput = luigi.IntParameter(50)
    hash_key = 'from_id'
    hash_key_type = STRING
    range_key = 'rank'
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def table_name(self):
        """
        Returns the table name to be created in DynamoDB.
        """
        # Append '-II' to distinguish between this table and the user-item table.
        return '%s-%s' % (self.dynamodb_table_name, 'II')

    def requires(self):
        """
        Tell Luigi to run the UserItemRecs task before this task.
        """
        return [UserItemRecs(input_base_path=self.input_base_path,
                             output_base_path=self.output_base_path)]


class CreateUITable(dynamodb.CreateDynamoDBTable):
    """
    This task creates a DynamoDB table for storing the user-item recommendations.
    """
    # The input_base_path used by the RetailPigscriptTasks.
    # This parameter is unused in this task, but it must be passed up the task dependency graph.
    input_base_path = luigi.Parameter()

    # As this task is creating a DynamoDB table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # Parameters required for creating a DynamoDB table
    dynamodb_table_name = luigi.Parameter()
    read_throughput = luigi.IntParameter(1)
    write_throughput = luigi.IntParameter(50)
    hash_key = 'from_id'
    hash_key_type = STRING
    range_key = 'rank'
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def table_name(self):
        """
        Returns the table name to be created in DynamoDB.
        """
        # Append '-UI' to distinguish between this table and the item-item table.
        return '%s-%s' % (self.dynamodb_table_name, 'UI')

    def requires(self):
        """
        Tell Luigi to run the UserItemRecs task before this task.
        """
        return [UserItemRecs(input_base_path=self.input_base_path,
                             output_base_path=self.output_base_path)]


class WriteDynamoDBTables(RetailPigscriptTask):
    """
    This task runs the 04-write-results-to-dynamodb.pig pigscript which writes the recommendations to the
    previously created DynamoDB tables.
    """

    # Base name of the DynamoDB tables where the recommendations will be written.
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that both the CreateUITable and CreateIITable tasks need to be completed
        before running this task.
        """
        return [CreateUITable(input_base_path=self.input_base_path,
                              output_base_path=self.output_base_path,
                              dynamodb_table_name=self.dynamodb_table_name),
                CreateIITable(input_base_path=self.input_base_path,
                              output_base_path=self.output_base_path,
                              dynamodb_table_name=self.dynamodb_table_name)]

    def script_output(self):
        """
        Because the recommendations are being written directly to DynamoDB there is no
        S3 output for this task.
        """
        return []

    def parameters(self):
        return {'II_TABLE': '%s-%s' % (self.dynamodb_table_name, 'II'),
                'UI_TABLE': '%s-%s' % (self.dynamodb_table_name, 'UI'),
                'OUTPUT_PATH': self.output_base_path,
                'AWS_ACCESS_KEY_ID': configuration.get_config().get('dynamodb', 'aws_access_key_id'),
                'AWS_SECRET_ACCESS_KEY': configuration.get_config().get('dynamodb', 'aws_secret_access_key')}

    def script(self):
        return '04-write-results-to-dynamodb'


class UpdateIITableThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    This task will ramp down the writes and/or up the reads to make the table
    ready for production use.

    The read/write throughput required to load a DynamoDB table is very different from
    the throughput required to serve your recommendations.  As AWS charges more for higher
    DynamoDB throughput this task can save a significant amount of money by ensuring
    you are only ever paying for the throughput you need.
    """

    # The target read throughput of the item-item dynamodb table
    read_throughput = luigi.IntParameter(1)

    # The target write throughput of the item-item dynamodb table
    write_throughput = luigi.IntParameter(1)

    # Not used in this task, but passed through for earlier tasks to use.
    input_base_path = luigi.Parameter()

    # As this task is only updating a DynamoDB table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # Base name of the DynamoDB table.
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that the WriteDynamoDBTables task needs to be completed
        before running this task.
        """
        return [WriteDynamoDBTables(input_base_path=self.input_base_path,
                                    output_base_path=self.output_base_path,
                                    dynamodb_table_name=self.dynamodb_table_name)]

    def table_name(self):
        # Append '-II' to distinguish between this table and the user-item table.
        return '%s-%s' % (self.dynamodb_table_name, 'II')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))


class UpdateUITableThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    This task will ramp down the writes and/or up the reads to make the table
    ready for production use.
    """

    # The target read throughput of the user-item dynamodb table
    read_throughput = luigi.IntParameter(1)

    # The target write throughput of the user-item dynamodb table
    write_throughput = luigi.IntParameter(1)

    # Not used in this task, but passed through for earlier tasks to use.
    input_base_path = luigi.Parameter()

    # As this task is only updating a DynamoDB table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # Base name of the DynamoDB table.
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that the UpdateIITableThroughput task needs to be completed
        before running this task.
        """
        return [UpdateIITableThroughput(input_base_path=self.input_base_path,
                                        output_base_path=self.output_base_path,
                                        dynamodb_table_name=self.dynamodb_table_name)]

    def table_name(self):
        # Append '-UI' to distinguish between this table and the item-item table.
        return '%s-%s' % (self.dynamodb_table_name, 'UI')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))


class SanityTestIITable(dynamodb.SanityTestDynamoDBTable):
    """
    This task will run a sanity test against our newly created item-item DynamoDB
    table by checking if it contains some expected data.
    """

    # The primary hash key for the DynamoDB table
    hash_key = 'from_id'

    # Not used in this task, but passed through for earlier tasks to use.
    input_base_path = luigi.Parameter()

    # As this task is only reading from a DynamoDB table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # Base name of the DynamoDB table.
    dynamodb_table_name = luigi.Parameter()

    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'II')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def ids(self):
        """
        Returns a list of item ids that we expect to be in the result data.
        """
        return ["the sixth sense", "48 hours", "friday the thirteenth", "the paper chase", "la femme nikita"]

    def requires(self):
        """
        Tell Luigi that the UpdateUITableThroughput task needs to be completed
        before running this task.
        """
        return [UpdateUITableThroughput(input_base_path=self.input_base_path,
                                        output_base_path=self.output_base_path,
                                        dynamodb_table_name=self.dynamodb_table_name)]


class SanityTestUITable(dynamodb.SanityTestDynamoDBTable):
    """
    This task will run a sanity test against our newly created user-item DynamoDB table
    by checking if it contains some expected data.
    """

    # The primary hash key for the DynamoDB table
    hash_key = 'from_id'

    # Not used in this task, but passed through for earlier tasks to use.
    input_base_path = luigi.Parameter()

    # As this task is only reading from a DynamoDB table and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # Base name of the DynamoDB table.
    dynamodb_table_name = luigi.Parameter()

    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'UI')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def ids(self):
        """
        Returns a list of user ids that we expect to be in the result data.
        """
        return ["90a9f83e789346fdb684f58212e355e0", "7c5ed8aacdb746f9b595bda2638de0dc",
                "bda100dcd4c24381bc24112d4ce46ecf", "f8462202b59e4e6ea93c09c98ecddb9c",
                "e65228e3b8364cb483361a81fe36e0d1"]

    def requires(self):
        """
        Tell Luigi that the SanityTestIITable task needs to be completed
        before running this task.
        """
        return [SanityTestIITable(input_base_path=self.input_base_path,
                                  output_base_path=self.output_base_path,
                                  dynamodb_table_name=self.dynamodb_table_name)]


class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    This is the very last task in the pipeline.  It will shut down all active
    clusters that are not currently running jobs.
    """

    # These parameters are not used by this task, but passed through for earlier tasks to use.
    input_base_path = luigi.Parameter()
    dynamodb_table_name = luigi.Parameter()

    # As this task is only shutting down clusters and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()


    def requires(self):
        """
        Tell Luigi that the SanityTestUITable task needs to be completed
        before running this task.
        """
        return [SanityTestUITable(input_base_path=self.input_base_path,
                                  output_base_path=self.output_base_path,
                                  dynamodb_table_name=self.dynamodb_table_name)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]


if __name__ == "__main__":
    """
    We tell Luigi to run the last task in the task dependency graph.  Luigi will then
    work backwards to find any tasks with its requirements met and start from there.

    The first time this pipeline is run the only task with its requirements met will be
    GenerateSignals which will find the required input files in S3.
    """
    luigi.run(main_task_cls=ShutdownClusters)
