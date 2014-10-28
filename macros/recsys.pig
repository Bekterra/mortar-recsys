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

register 'datafu-0.0.10.jar';
register 'trove4j-3.0.3.jar';
register 'recsys-udfs.jar';

define recsys__Enumerate 
    datafu.pig.bags.Enumerate('1');

register 'recsys.py' using jython as recsys_udfs;

----------------------------------------------------------------------------------------------------
/*
 * This file contains the basic macros used in macros/recommenders.pig for the various steps of 
 * building recommendations.
 *
 */
----------------------------------------------------------------------------------------------------

/*
 * This is the first step in the Mortar recommendation system.
 *
 * Build a weighted graph of item-item links from a collection of user-item signals.
 *
 * Algorithmically, this is "contracting" the bipartite user-item graph into a regular
 * graph of item similarities. If a user U has affinity with items I1 and I2 of weights
 * W1 and W2 respectively, than a link * between I1 and I2 is formed with the weight
 * MIN(W1, W2).
 *
 * Input:
 *      ui_signals: { (user:chararray, item:chararray, weight:float} )
 *      logistic_param: float       Influences how multiple links between a user and item are
 *                                  combined.  See params/README.md for details.
 *      min_link_weight: float      For performance any item-item links lower than this value
 *                                  will be removed.  See params/README.md for details.
 *      max_links_per_user: int     For performance only keep the top [max_links_per_user link]
 *                                  for an individual user.  See params/README.md for details.
 *
 * Output:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float) }
 *      item_weights: { (item:int, overall_weight:float) }
 *                                 item_weights contains an overall popularity weight for each item.
 */
define recsys__BuildItemItemGraph(ui_signals, logistic_param, min_link_weight, max_links_per_user)
returns ii_links, item_weights {

    define recsys__UserItemToItemItemGraphBuilder
        com.mortardata.recsys.UserItemToItemItemGraphBuilder();
    define recsys__FilterItemItemLinks
        com.mortardata.recsys.FilterItemItemLinks('$min_link_weight');

    ui_signals      =   filter $ui_signals by user is not null and item is not null;

    -- Aggregate events by (user,item) and sum weights to get one weight for each user-item combination.
    ui_agg          =   foreach (group ui_signals by (user, item)) generate
                            flatten(group) as (user, item),
                            (float) SUM($1.weight) as weight;

    -- Apply logistic function to user-item weights so a user with tons of events for the same item
    -- faces diminishing returns.
    ui_scaled       =   foreach ui_agg generate
                            user, item,
                            (float) recsys_udfs.logistic_scale(weight, $logistic_param)
                            as weight;

    -- Sum up the scaled weights for each item to determine its overall popularity weight.
    item_weights_tmp =   foreach (group ui_scaled by item) generate
                            group as item, (float) SUM($1.weight) as overall_weight, $1 as ui;
    $item_weights    =   foreach item_weights_tmp generate item, overall_weight;

    -- Drop items that don't meet the minimum weight.
    ui_filt         =   foreach (filter item_weights_tmp by overall_weight >= $min_link_weight) generate
                            flatten(ui) as (user, item, weight);

    -- Turn the user-item links into an item-item graph where each link is above the
    -- minimum required weight.
    ii_link_terms  =   foreach (group ui_filt by user) {
                            top_for_user = TOP($max_links_per_user, 2, $1);
                            generate flatten(recsys__UserItemToItemItemGraphBuilder(top_for_user));
                        }
    $ii_links      =   foreach (group ii_link_terms by item_A) generate
                            group as item_A,
                            flatten(recsys__FilterItemItemLinks($1))
                                  as (item_B, weight);
};

/*
 * This is the second step in the Mortar recommendation system.
 *
 * Take a weighted item-item graph and adjust the weights based on the popularity of the item
 * linked to.  Without accounting for this, popular items will be considered "most-similar" for
 * every other item, since users of the other items frequently interact with the popular item.
 *
 * This macro uses Bayes theorem to avoid this problem, and also scaled the
 * item-to-item links to all be within the range [0, 1]. It sets the similarity
 * of items A and B to be an estimate of the probability that a random user U
 * will interact with A given that they interacted with B. In mathematical notation,
 * it similarity(A, B) = P(A | B). This way, if B is very popular, you need a lot
 * of users co-interacting with it and A for the link to be statistically significant.
 *
 * This estimation breaks down if B is very unpopular, with only a few users interacting with it.
 * If B only has 2 users and they all interacted with A, that is most likely due to chance, not similarity.
 * The macro therefore takes a Bayesian Prior, which guards against these small sample sizes.
 * Intuitively, it represents a number of "pseudo-observations" of the non-similarity of A and B;
 * or in other words, A is "innocent of B until proven guilty beyond a reasonable doubt".
 *
 * Input:
 *      ii_links_raw: { (item_A:chararray, item_B:chararray, weight:float) }
 *      item_weights: { (item:chararray, overall_weight:float) }
 *      prior: float                The prior guards the recommendations against the effects of items
 *                                  with a small sample size.  See params/README.md for details.
 *
 * Output:
 *      ii_links_bayes: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float) }
 *
 *      raw_weight: The original non-adjusted weight of the item-item link.
 */
define recsys__AdjustItemItemGraphWeight(ii_links_raw, item_weights, prior)
returns ii_links_bayes {

    $ii_links_bayes =   foreach (join $item_weights by item, $ii_links_raw by item_B) generate
                            item_A as item_A,
                            item_B as item_B,
                            (float) (weight / (overall_weight + $prior))
                            as weight,
                            weight as raw_weight;
};

/*
 * This is the third step in the Mortar recommendation system.
 *
 * After any domain-specific link boosting/penalization has been applied to the item-item graph
 * use that graph to generate recommendations ranked by the weight of the link.
 *
 * Instead of restricting recommendations to direct neighbours of an item, this will
 * find the best recommendations in the 2-neighbourhood of an item.  This should improve
 * recommendations and help fill out the recommendation sets for items with small sample sizes.
 *
 * When following paths, distance is defined to be the inverse of the similarity weights.
 *
 * This has the effect that if there is a path from items A -> B -> C that has a total
 * distance of less than A -> D, then the former path is recognized is more relevant;
 * that is, the link A -> C will be ranked higher than A -> D.
 *
 * In the output, the field "raw_weight" will be null if the link is indirect.
 *
 * Input:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float) }
 *      num_recs: int
 *      initial_nhood_size: int     For performance reasons you can prune an item's direct links
 *                                  before performing the shortest path search.  This should always
 *                                  be <= to num_recs.
 * Output:
 *      item_recs: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float, rank:int) }
 */
define recsys__BuildItemItemRecommendationsFromGraph( ii_links, initial_nhood_size, num_recs)
returns item_recs {

    graph, paths        =   recsys__InitShortestPaths($ii_links, $initial_nhood_size);

    two_step_terms      =   foreach (join graph by item_B, paths by item_A) generate
                                graph::item_A as item_A,
                                paths::item_B as item_B,
                                graph::dist + paths::dist as dist,
                                (paths::item_A == paths::item_B ?
                                    graph::raw_weight : paths::raw_weight) as raw_weight;

    shortest_paths_dups =   foreach (group two_step_terms by (item_A, item_B)) generate
                                flatten(recsys_udfs.best_path($1))
                                as (item_A, item_B, dist, raw_weight);
    shortest_paths_full =   filter shortest_paths_dups by item_A != item_B;

    -- jython udf returns doubles so recast to float
    shortest_paths      =   foreach shortest_paths_full generate
                                item_A, item_B, (float) dist, (float) raw_weight;

    nhoods_tmp          =   foreach (group shortest_paths by item_A) {
                                ordered = order $1 by dist asc;
                                    top = limit ordered $num_recs;
                                generate flatten(recsys__Enumerate(top))
                                      as (item_A, item_B, dist, raw_weight, rank);
                            }

    $item_recs          =   foreach nhoods_tmp generate
                                item_A, item_B, 1.0f / dist as weight, raw_weight, (int) rank;
};


/*
 * Helper method for recsys__BuildItemItemRecommendationsFromGraph.
 *
 * Construct distance and path graphs for use in the shortest path algorithm.
 *
 * Input:
 *      ii_links: { (item_A:chararray, item_B:chararray, weight:float, raw_weight:float) }
 *      num_recs: int
 *
 * Output:
 *       graph: { (item_A:chararray, item_B:chararray, dist:float, raw_weight:float) }
 *       paths: { (item_A:chararray, item_B:chararray, dist:float, raw_weight:float) }
 */
define recsys__InitShortestPaths(ii_links, num_recs) returns graph, paths {

    distance_mat        =   foreach $ii_links generate
                                item_A, item_B, 1.0f / weight as dist, raw_weight;

    $graph              =   foreach (group distance_mat by item_A) {
                                sorted = order $1 by dist asc;
                                   top = limit sorted $num_recs;
                                generate flatten(top)
                                      as (item_A, item_B, dist, raw_weight);
                            }

    graph_copy          =   foreach $graph generate item_A, item_B, dist, null as raw_weight;
    dest_verts_dups     =   foreach graph_copy generate item_B as id;
    dest_verts          =   distinct dest_verts_dups;
    self_loops          =   foreach dest_verts generate
                                id as item_A, id as item_B, 0.0f as dist, null as raw_weight;
    $paths              =   union graph_copy, self_loops;
};

----------------------------------------------------------------------------------------------------

/*
 * This macro takes links between users and items, and the item-to-item recommendations,
 * and generates "user neighborhoods" consisting of all the items recommended for any item
 * the user has a link to. It then
 *     1) applies a filter so that users are not recommended items they have already seen
 *     2) if an item is recommended multiple times, takes the highest-scoring of those recs
 *     3) limits the recs to the top N
 *
 * Input:
 *      user_item_signals: { (user:chararray, item:chararray, weight:float) }
 *      item_item_recs: { (item_A:chararray, item_B:chararray, weight:float) }
 *      num_recs: int
 *      diversity_adjust: 'false' or 'true'     An option to try and generate more diverse recommendations.
 *                                              See params/README.md for more details.
 *
 * Output:
 *      user_item_recs: { (user:chararray, item:chararray, weight:flaot, reason_item:chararray,
 *                         user_reason_item_weight:float, item_reason_item_weight:float, rank:int) }
 *
 *      reason_item: The item the user interacted with that generated this recommendation
 *      user_reason_item_weight: The weight the user had with the reason_item
 *      item_reason_item_weight: The original weight the item recommended had with the reason_item
 *
 */
define recsys__BuildUserItemRecommendations(user_item_signals, item_item_recs, num_recs, diversity_adjust) 
returns ui_recs {

    define recsys__RefineUserItemRecs
        com.mortardata.recsys.RefineUserItemRecs('$num_recs', '$diversity_adjust');

    user_recs_tmp   =   foreach (join $user_item_signals by item,
                                      $item_item_recs by item_A) generate
                                            user as user,
                                          item_B as item,
                            (float)
                            SQRT(
                                  ($user_item_signals::weight > 0 ?
                                     $user_item_signals::weight : 0)
                                 * $item_item_recs::weight) as weight,
                                          item_A as reason,
                      $user_item_signals::weight as user_link,
                                      raw_weight as item_link;

    ui_recs_full    =   foreach (cogroup $user_item_signals by user, user_recs_tmp by user) generate
                            flatten(recsys__RefineUserItemRecs($user_item_signals, user_recs_tmp))
                            as (user, item, weight,
                                reason_item, user_reason_item_weight, item_reason_item_weight,
                                diversity_adj_weight, rank);
    $ui_recs        =   foreach ui_recs_full generate $0..$5, $7;
};
