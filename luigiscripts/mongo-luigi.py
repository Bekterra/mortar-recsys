import luigi
from luigi import configuration
from luigi.s3 import S3Target, S3PathTask

from mortar.luigi import mongodb
from mortar.luigi import mortartask


"""
This luigi pipeline runs the Last.fm example, pulling data from mongo and putting the
results in mongo.  Luigi tracks progress by writing intermediate data to S3.

To run, set the following secure project configuration variables:

    mortar config:set CONN=mongodb://<username>:<password>@<host>:<port>
    mortar config:set DB=<databasename>
    mortar config:set COLLECTION=<collectionname>

Task Order:
    GenerateSignals
    ItemItemRecs
    UserItemRecs
    WriteMongoDBCollections
    SanityTestIICollection
    SanityTestUICollection
    ShutdownClusters

To run:

    mortar luigi luigiscripts/mongo-luigi.py \
      --output-base-path "s3://<your-s3-bucket>/mongo-lastfm" \
      --mongodb-output-collection-name "<collection-name>"
"""


def create_full_path(base_path, sub_path):
    """
    Helper function for constructing paths.
    """
    return '%s/%s' % (base_path, sub_path)


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
    cluster_size = luigi.IntParameter(default=15)

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
    This task runs the 01-mongo-generate-signals.pig pigscript.
    """

    def requires(self):
        """
        The requires method is how you build your dependency graph in Luigi.  Luigi will not
        run this task until all tasks returning in this list are complete.

        GenerateSignals is the first task in our pipeline.  An empty list is
        returned to tell Luigi that this task is always ready to run.
        """
        return []

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
        return {'CONN': configuration.get_config().get('mongodb', 'mongo_conn'),
                'DB': configuration.get_config().get('mongodb', 'mongo_db'),
                'COLLECTION': configuration.get_config().get('mongodb', 'mongo_input_collection'),
                'OUTPUT_PATH': self.output_base_path}

    def script(self):
        """
        This is the name of the pigscript that will be run.

        You can find this script in the pigscripts directory of your Mortar project.
        """
        return 'mongo/01-mongo-generate-signals'


class ItemItemRecs(LastfmPigscriptTask):
    """
    This task runs the 02-item-item-recs.pig pigscript.
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
        return 'mongo/02-mongo-item-item-recs'


class UserItemRecs(LastfmPigscriptTask):
    """
    This task runs the 03-user-item-recs.pig pigscript
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
        return 'mongo/03-mongo-user-item-recs'


class WriteMongoDBCollections(LastfmPigscriptTask):
    """
    This task runs the 04-write-results-to-mongodb.pig pigscript which writes the
    recommendations to MongoDB.
    """

    # The name of the MongoDB collection where results will be written.
    mongodb_output_collection_name = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that the UserItemRecs task needs to be completed
        before running this task.
        """
        return [UserItemRecs(output_base_path=self.output_base_path)]

    def script_output(self):
        """
        Because the recommendations are being written directly to MongoDB there is no
        S3 output for this task.
        """
        return []

    def parameters(self):
        return {'CONN': configuration.get_config().get('mongodb', 'mongo_conn'),
                'DB': configuration.get_config().get('mongodb', 'mongo_db'),
                'II_COLLECTION': '%s_%s' % (self.mongodb_output_collection_name, 'II'),
                'UI_COLLECTION': '%s_%s' % (self.mongodb_output_collection_name, 'UI'),
                'OUTPUT_PATH': self.output_base_path
               }

    def script(self):
        return 'mongo/04-write-results-to-mongodb'


class SanityTestIICollection(mongodb.SanityTestMongoDBCollection):
    """
    This task will run a sanity test against our newly created item-item MongoDB
    collection by checking if it contains some expected data.
    """

    # Id field to check.
    id_field = 'from_id'

    # As this task is only reading from a MongoDB collection and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The name of the MongoDB collection where results were written.
    mongodb_output_collection_name = luigi.Parameter()

    def collection_name(self):
        # Append '-II' to distinguish between this collection and a
        # user-item collection.
        return '%s_%s' % (self.mongodb_output_collection_name, 'II')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def ids(self):
        """
        Returns a list of item ids that we expect to be in the result data.
        """
        return ["the beatles", "miley cyrus", "yo-yo ma", "ac dc", "coldplay"]

    def requires(self):
        """
        Tell Luigi that the WriteMongoDBCollections task needs to be completed
        before running this task.
        """
        return [WriteMongoDBCollections(output_base_path=self.output_base_path,
                                        mongodb_output_collection_name=self.mongodb_output_collection_name)]


class SanityTestUICollection(mongodb.SanityTestMongoDBCollection):
    """
    This task will run a sanity test against our newly created user-item MongoDB
    collection by checking if it contains some expected data.
    """

    #Id field to check
    id_field = 'from_id'

    # As this task is only reading from a MongoDB collection and not generating any output data,
    # this S3 location is used to store a 'token' file indicating when the task has
    # been completed.
    output_base_path = luigi.Parameter()

    # The name of the MongoDB collection where results were written.
    mongodb_output_collection_name = luigi.Parameter()

    def collection_name(self):
        # Append '-UI' to distinguish between this collection and a
        # item-item collection.
        return '%s_%s' % (self.mongodb_output_collection_name, 'UI')

    def output_token(self):
        return S3Target(create_full_path(self.output_base_path, self.__class__.__name__))

    def ids(self):
        """
        Returns a list of item ids that we expect to be in the result data.
        """
        return ["faf0805d215993c5ff261e58a5358131cf2b2a60", "faf0aa22d8621be9ed7222e3867caf1a560d8785", "faf0c313b1952ba6d83f390dedea81379eed881a", "faf12b4c90e90cb77adc284f0a5970decad86bde", "faf18c1cca1a4172011334821e0c124a7eedfa50"]

    def requires(self):
        """
        Tell Luigi that the SanityTestIICollection task needs to be completed
        before running this task.
        """
        return [SanityTestIICollection(output_base_path=self.output_base_path,
                                       mongodb_output_collection_name=self.mongodb_output_collection_name)]


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
    mongodb_output_collection_name = luigi.Parameter()

    def requires(self):
        """
        Tell Luigi that the SanityTestUICollection task needs to be completed
        before running this task.
        """
        return [SanityTestUICollection(output_base_path=self.output_base_path,
                                       mongodb_output_collection_name=self.mongodb_output_collection_name)]

    def output(self):
        return [S3Target(create_full_path(self.output_base_path, self.__class__.__name__))]


if __name__ == "__main__":
    """
    We tell Luigi to run the last task in the task dependency graph.  Luigi will then
    work backwards to find any tasks with its requirements met and start from there.

    The first time this pipeline is run the only task with its requirements met will be
    GenerateSignals.
    """
    luigi.run(main_task_cls=ShutdownClusters)
