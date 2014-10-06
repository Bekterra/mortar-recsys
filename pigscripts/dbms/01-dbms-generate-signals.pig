import 'recommenders.pig';

/**
 *  Generates artist recommendations based off of last.fm data provided by
 *  http://www.dtic.upf.edu/~ocelma/MusicRecommendationDataset/lastfm-360K.html
 */

raw_input  =    load '$INPUT_SIGNALS' using PigStorage()
                        as (user: chararray, item_id: chararray, item: chararray, weight: float);

user_signals = foreach raw_input generate user, item, weight;

user_signals = filter user_signals by user is not null and item is not null and weight is not null;

rmf $OUTPUT_PATH/user_signals;
store user_signals into '$OUTPUT_PATH/user_signals' using PigStorage();