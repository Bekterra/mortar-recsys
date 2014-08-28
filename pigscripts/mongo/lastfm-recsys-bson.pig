import 'recommenders.pig';
import 'recsys_helper.pig';

/**
 *  Generates artist recommendations based off of last.fm data provided by
 *  http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html
 *
 *  To run this script you will need to have the last.fm data in bson form
 *  stored in s3.
*/

SET bson.split.read_splits false;

%default BSON_INPUT_PATH 's3://<bucket>'
%default OUTPUT_PATH 's3://mortar-example-output-data/$MORTAR_EMAIL_S3_ESCAPED/mongo/lastfm-recommendations'

/******* Load Data **********/

raw_input =  load '$BSON_INPUT_PATH/lastfm_plays.bson'
            using com.mongodb.hadoop.pig.BSONLoader('mongo_id', '
                      mongo_id:chararray,
                      user:chararray,
                      artist_name:chararray,
                      num_plays:int
                  ');

-- The more times the user plays an artist the stronger the signal.
input_signals = foreach raw_input generate
                 user,
                 artist_name as item,
                 num_plays as weight:int;


/******* Use Mortar recommendation engine to convert signals to recommendations **********/

item_item_recs = recsys__GetItemItemRecommendations(input_signals);
user_item_recs = recsys__GetUserItemRecommendations(input_signals, item_item_recs);


/******* Store recommendations back into Mongo **********/

--  If your output folder exists already, hadoop will refuse to write data to it.
rmf $OUTPUT_PATH/item_item_recs;
rmf $OUTPUT_PATH/user_item_recs;

store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
store user_item_recs into '$OUTPUT_PATH/user_item_recs' using PigStorage();
