import 'recommenders.pig';
import 'recsys_helper.pig';

-- Load the user signals from 01-generate-signals.pig
input_signals = load '$OUTPUT_PATH/input_signals' using PigStorage()
                        as (user: chararray, item: chararray, weight: float);

/******* Use Mortar recommendation engine to convert signals to recommendations **********/

-- Call the default Mortar recommender algorithm on the user-item data.
item_item_recs = recsys__GetItemItemRecommendations(input_signals);

rmf $OUTPUT_PATH/item_item_recs;
store item_item_recs into '$OUTPUT_PATH/item_item_recs' using PigStorage();
