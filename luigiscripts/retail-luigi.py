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

# helper function
def create_full_path(base_path, sub_path):
    return '%s/%s' % (base_path, sub_path)

class RetailPigscriptTask(mortartask.MortarProjectPigscriptTask):
    # s3 path to the folder where the input data is located
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # cluster size to use
    cluster_size = luigi.IntParameter(default=2)

    def project(self):
        """
        Name of the mortar project to run.
        """
        return MORTAR_PROJECT

    def token_path(self):
        return self.output_base_path

    def default_parallel(self):
        return (self.cluster_size - 1) * mortartask.NUM_REDUCE_SLOTS_PER_MACHINE


class GenerateSignals(RetailPigscriptTask):
    """
    Runs the 01-generate-signals.pig Pigscript
    """

    def requires(self):
        return [S3PathTask(create_full_path(self.input_base_path, 'purchases.json')),
                S3PathTask(create_full_path(self.input_base_path, 'wishlists.json'))]

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user_signals'))]

    def parameters(self):
        return {'INPUT_PATH_PURCHASES': create_full_path(self.input_base_path, 'purchases.json'),
                'INPUT_PATH_WISHLIST': create_full_path(self.input_base_path, 'wishlists.json'),
                'OUTPUT_PATH': self.output_base_path}

    def script(self):
        """
        Name of the script to run.
        """
        return '01-generate-signals'


class ItemItemRecs(RetailPigscriptTask):
    """
    Runs the 02-item-item-recs.pig Pigscript
    """

    def requires(self):
        return [GenerateSignals(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]

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
        return '02-item-item-recs'

class UserItemRecs(RetailPigscriptTask):
    """
    Runs the 03-user-item-recs.pig Pigscript
    """

    def requires(self):
        return [ItemItemRecs(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]

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
        return '03-user-item-recs'

class CreateIITable(dynamodb.CreateDynamoDBTable):
    """
    Creates a DynamoDB table for storing the item-item recommendations
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    # initial read throughput of the table
    read_throughput = luigi.IntParameter(1)

    # initial write throughput of the table
    write_throughput = luigi.IntParameter(10)

    # primary hash key for the DynamoDB table
    hash_key = 'from_id'

    # type of the hash key
    hash_key_type = STRING

    # range key for the DynamoDB table
    range_key = 'rank'

    # type of the range key
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # append '-II' to distinguish between this and the user-item table
    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'II')

    def requires(self):
        return [UserItemRecs(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]

class CreateUITable(dynamodb.CreateDynamoDBTable):
    """
    Creates a DynamoDB table for storing the user-item recommendations
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    # initial read throughput of the table
    read_throughput = luigi.IntParameter(1)

    # initial write throughput of the table
    write_throughput = luigi.IntParameter(10)

    # primary hash key for the DynamoDB table
    hash_key = 'from_id'

    # type of the hash key
    hash_key_type = STRING

    # range key for the DynamoDB table
    range_key = 'rank'

    # type of the range key
    range_key_type = NUMBER

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # append '-UI' to distinguish between this and the item-item table
    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'UI')

    def requires(self):
        return [UserItemRecs(input_base_path=self.input_base_path, output_base_path=self.output_base_path)]


class WriteDynamoDBTables(RetailPigscriptTask):
    """
    Runs the 04-write-results-to-dynamodb.pig Pigscript
    """

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        return [CreateUITable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name),
                CreateIITable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name)]

    def script_output(self):
        return []

    def parameters(self):
        return {'II_TABLE': '%s-%s' % (self.dynamodb_table_name, 'II'),
                'UI_TABLE': '%s-%s' % (self.dynamodb_table_name, 'UI'),
                'OUTPUT_PATH': self.output_base_path,
                'AWS_ACCESS_KEY_ID': configuration.get_config().get('dynamodb', 'aws_access_key_id'),
                'AWS_SECRET_ACCESS_KEY': configuration.get_config().get('dynamodb', 'aws_secret_access_key')}

    def script(self):
        """
        Name of the script to run.
        """
        return '04-write-results-to-dynamodb'

class UpdateIITableThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    After writing to the table, ramp down the writes and/or up the writes to make the table
    ready for production.
    """

    # target read throughput of the dynamodb table
    read_throughput = luigi.IntParameter(1)

    # target write throughput of the dynamodb table
    write_throughput = luigi.IntParameter(1)

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        return [WriteDynamoDBTables(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name)]

    # append '-II' to distinguish between this and the user-item table
    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'II')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

class UpdateUITableThroughput(dynamodb.UpdateDynamoDBThroughput):
    """
    After writing to the table, ramp down the writes and/or up the writes to make the table
    ready for production.
    """

    # target read throughput of the dynamodb table
    read_throughput = luigi.IntParameter(1)

    # target write throughput of the dynamodb table
    write_throughput = luigi.IntParameter(1)

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        return [UpdateIITableThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name)]

    # append '-UI' to distinguish between this and the item-item table
    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'UI')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

class SanityTestIITable(dynamodb.SanityTestDynamoDBTable):
    """
    Check that the DynamoDB table contains expected data
    """

    # primary hash key for the DynamoDB table
    hash_key = 'from_id'

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    # append '-II' to distinguish between this and the item-item table
    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'II')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # sentinel ids expected to be in the result data
    def ids(self):
        return ["the sixth sense", "48 hours", "friday the thirteenth", "the paper chase", "la femme nikita"]

    def requires(self):
        return [UpdateUITableThroughput(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name)]


class SanityTestUITable(dynamodb.SanityTestDynamoDBTable):
    """
    Check that the DynamoDB table contains expected data
    """

    # primary hash key for the DynamoDB table
    hash_key = 'from_id'

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # name of the table
    dynamodb_table_name = luigi.Parameter()

    # append '-UI' to distinguish between this and the item-item table
    def table_name(self):
        return '%s-%s' % (self.dynamodb_table_name, 'UI')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # sentinel ids expected to be in the result data
    def ids(self):
        return ["90a9f83e789346fdb684f58212e355e0", "7c5ed8aacdb746f9b595bda2638de0dc", "bda100dcd4c24381bc24112d4ce46ecf",
                "f8462202b59e4e6ea93c09c98ecddb9c", "e65228e3b8364cb483361a81fe36e0d1"]

    def requires(self):
        return [SanityTestIITable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name)]

class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    When the pipeline is completed, shut down all active clusters not currently running jobs
    """

    # unused, but must be passed through
    input_base_path = luigi.Parameter()

    # s3 path to the output folder
    output_base_path = luigi.Parameter()

    # unused, but must be passed through
    dynamodb_table_name = luigi.Parameter()

    def requires(self):
        return [SanityTestUITable(input_base_path=self.input_base_path, output_base_path=self.output_base_path, dynamodb_table_name=self.dynamodb_table_name)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]

if __name__ == "__main__":
    luigi.run(main_task_cls=ShutdownClusters)
