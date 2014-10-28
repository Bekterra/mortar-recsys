/*
 * Copyright 2014 Mortar Data Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "as is" Basis,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

----------------------------------------------------------------------------------------------------
/**
 * This file contains macros that can be useful when turning or modifying your
 * recommendation system.
 */
----------------------------------------------------------------------------------------------------

register 'datafu-1.2.0.jar';

/**
 * This macro helps you tune the BAYESIAN_PRIOR parameter for the recommender.  See
 * params/README.md for more details.
 *
 * Returns a tuple giving quantiles over the distribution of item popularity weights.
 *
 * Input:
 *      ui_signals: { (user:chararray, item:chararray, weight:float} )
 *
 * Output:
 *      item_quants: (min, 5% quantile, 10% quantile, ..., 95% quantile, max)
 */
define recsys__GetQuantiles(ui_signals) returns item_quants {
    define Quantiles datafu.pig.stats.StreamingQuantile('21');

    item_weights  = foreach (group $ui_signals by item) generate
                       group as item, (float) SUM($1.weight) as weight;

    $item_quants = foreach (group item_weights all) generate Quantiles($1.weight);
};
