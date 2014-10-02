import abc

import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import mongodb
from mortar.luigi import mortartask
from mortar.luigi import target_factory


"""
This luigi pipeline runs the Last.fm example, pulling data from mongo and putting the
results in mongo.  Luigi tracks progress by writing intermediate data to S3.

To run, replace the value of MORTAR_PROJECT below with your actual project name. 
Also, set the following secure project configuration variables:

    mortar config:set CONN=mongodb://<username>:<password>@<host>:<port>
    mortar config:set DB=<databasename>
    mortar config:set COLLECTION=<collectionname>

Task Order:
    GenerateSignals
    ItemItemRecs
    UserItemRecs
    SanityTestIICollection
    SanityTestUICollection
    ShutdownClusters

To run:
    
    mortar luigi luigiscripts/mongo-luigi.py \
      --output-base-path "s3://<your-s3-bucket>/mongo-lastfm" \
      --mongodb-output-collection-name "<collection-name>"
"""

# helper function
def create_full_path(base_path, sub_path):
    return '%s/%s' % (base_path, sub_path)

# REPLACE WITH YOUR PROJECT NAME
MORTAR_PROJECT = '<your-project>'

class LastfmPigscriptTask(mortartask.MortarProjectPigscriptTask):
    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # cluster size to use
    cluster_size = luigi.IntParameter(default=15)

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
    Runs the 01-mongo-generate-signals.pig Pigscript
    """

    def requires(self):
        return []

    def script_output(self):
        return [S3Target(create_full_path(self.output_base_path, 'user_signals'))]

    def parameters(self):
        return {'CONN': configuration.get_config().get('mongodb', 'mongo_conn'),
                'DB': configuration.get_config().get('mongodb', 'mongo_db'),
                'COLLECTION': configuration.get_config().get('mongodb', 'mongo_input_collection'),
                'OUTPUT_PATH': self.output_base_path}

    def script(self):
        """
        Name of the script to run.
        """
        return 'mongo/01-mongo-generate-signals'


class ItemItemRecs(LastfmPigscriptTask):
    """
    Runs the 02-mongo-item-item-recs.pig Pigscript
    """

    def requires(self):
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
        """
        Name of the script to run.
        """
        return 'mongo/02-mongo-item-item-recs'

class UserItemRecs(LastfmPigscriptTask):
    """
    Runs the 03-mongo-user-item-recs.pig Pigscript
    """

    def requires(self):
        return [ItemItemRecs(output_base_path=self.output_base_path)]

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
        return 'mongo/03-mongo-user-item-recs'

class WriteMongoDBCollections(LastfmPigscriptTask):
    """
    Runs the 04-write-results-to-mongodb.pig Pigscript
    """

    # name of the collection
    mongodb_output_collection_name = luigi.Parameter()

    def requires(self):
        return [UserItemRecs(output_base_path=self.output_base_path)]

    def script_output(self):
        return []

    def parameters(self):
        return {'CONN': configuration.get_config().get('mongodb', 'mongo_conn'),
                'DB': configuration.get_config().get('mongodb', 'mongo_db'),
                'II_COLLECTION': '%s_%s' % (self.mongodb_output_collection_name, 'II'),
                'UI_COLLECTION': '%s_%s' % (self.mongodb_output_collection_name, 'UI'),
                'OUTPUT_PATH': self.output_base_path
               }

    def script(self):
        """
        Name of the script to run.
        """
        return 'mongo/04-write-results-to-mongodb'

class SanityTestIICollection(mongodb.SanityTestMongoDBCollection):
    """
    Check that the MongoDB collection contains expected data
    """

    #Id field to check
    id_field = 'from_id'

    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # name of the collection
    mongodb_output_collection_name = luigi.Parameter()

    # append '_II' to distinguish between this and the item-item collection
    def collection_name(self):
        return '%s_%s' % (self.mongodb_output_collection_name, 'II')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # sentinel ids expected to be in the result data
    def ids(self):
        return ["the beatles", "miley cyrus", "yo-yo ma", "ac dc", "coldplay"]

    def requires(self):
        return [WriteMongoDBCollections(output_base_path=self.output_base_path,
                                        mongodb_output_collection_name=self.mongodb_output_collection_name)]

class SanityTestUICollection(mongodb.SanityTestMongoDBCollection):
    """
    Check that the MongoDB collection contains expected data
    """

    #Id field to check
    id_field = 'from_id'

    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # name of the collection
    mongodb_output_collection_name = luigi.Parameter()

    # append '_UI' to distinguish between this and the item-item collection
    def collection_name(self):
        return '%s_%s' % (self.mongodb_output_collection_name, 'UI')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    # sentinel ids expected to be in the result data
    def ids(self):
        return ["faf0805d215993c5ff261e58a5358131cf2b2a60", "faf0aa22d8621be9ed7222e3867caf1a560d8785", "faf0c313b1952ba6d83f390dedea81379eed881a", "faf12b4c90e90cb77adc284f0a5970decad86bde", "faf18c1cca1a4172011334821e0c124a7eedfa50"]

    def requires(self):
        return [SanityTestIICollection(output_base_path=self.output_base_path,
                                       mongodb_output_collection_name=self.mongodb_output_collection_name)]

class ShutdownClusters(mortartask.MortarClusterShutdownTask):
    """
    When the pipeline is completed, shut down all active clusters not currently running jobs
    """

    # s3 path to the output folder used by luigi to track progress
    output_base_path = luigi.Parameter()

    # unused, but must be passed through
    mongodb_output_collection_name = luigi.Parameter()

    def requires(self):
        return [SanityTestUICollection(output_base_path=self.output_base_path,
                                       mongodb_output_collection_name=self.mongodb_output_collection_name)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]

if __name__ == "__main__":
    luigi.run(main_task_cls=ShutdownClusters)
