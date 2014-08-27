import 'recommenders.pig';
import 'recsys_helper.pig';

/**
 *  Generates artist recommendations based off of last.fm data provided by
 *  http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html
 *
 *  To run this script you will need to provide a parameter CONN with your mongo connection
 *  string.  To set in mortar do:
 *
 *      mortar config:set CONN=mongodb://<username>:<password>@<host>:<port>
 *
 *  You will also need to have the data loaded in MongoDB.  To do that you can use
 *  the pigscripts/mongo/load_lastfm_data_to_mongo.pig script.
*/

-- This needs to be set when running without admin access on your mongoDB cluster.
SET mongo.input.split.create_input_splits false;

%default DB 'mortar_demo'
%default COLLECTION 'lastfm_plays'

raw_input = 
    load '$CONN/$DB.$COLLECTION'
    using com.mongodb.hadoop.pig.MongoLoader('
           user:chararray,
           artist_name:chararray,
           num_plays:int
    ');


/******* Convert Data to Signals **********/

-- The more times the user plays an artist the stronger the signal.
input_signals = foreach raw_input generate
                 user,
                 artist_name as item,
                 num_plays as weight:int;


/******* Use Mortar recommendation engine to convert signals to recommendations **********/

item_item_recs = recsys__GetItemItemRecommendations(input_signals);
user_item_recs = recsys__GetUserItemRecommendations(input_signals, item_item_recs);


/******* Store recommendations back into Mongo **********/

%default II_COLLECTION 'item_item_recs'
%default UI_COLLECTION 'user_item_recs'

store item_item_recs into 
  '$CONN/$DB.$II_COLLECTION' using com.mongodb.hadoop.pig.MongoInsertStorage('','');

store user_item_recs into
  '$CONN/$DB.$UI_COLLECTION' using com.mongodb.hadoop.pig.MongoInsertStorage('','');

