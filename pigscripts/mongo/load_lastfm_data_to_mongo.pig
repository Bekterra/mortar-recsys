/**
 *  Load user-artist-plays data from Last.fm into mongoDB.  Data provided by: 
 *  http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html
 * 
 *  To run this script you will need to provide a parameter CONN
 *  with your mongo connection string. You will also need to
 *  provide parameters DB and COLLECTION with your mongo database and collection name.
 *  To set in mortar do:
 *
 *      mortar config:set CONN=mongodb://<username>:<password>@<host>:<port>
 *      mortar config:set DB=<databasename>
 *      mortar config:set COLLECTION=<collectionname>
 */

-- s3 path containing downloaded last.fm data file.
%default INPUT_PATH 's3://mortar-example-data/input/lastfm-dataset-360K/usersha1-artmbid-artname-plays.tsv'

-- Load the data
plays = load '$INPUT_PATH' using PigStorage('\t') 
            as (user:chararray, 
                artist_id:chararray,
                artist_name:chararray, 
                num_plays:int);

-- The full Last.fm data set is approximately 17 million documents.  Uncomment the following line to
-- load only a smaller subset of the data.
--plays = limit plays 1000000;

-- Store results to Mongo
store plays into '$CONN/$DB.$COLLECTION' using com.mongodb.hadoop.pig.MongoStorage();
