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

package com.mortardata.recsys;

import gnu.trove.map.hash.TCustomHashMap;
import gnu.trove.map.hash.TObjectFloatHashMap;
import gnu.trove.set.hash.THashSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;


public class RefineUserItemRecs extends EvalFunc<DataBag> {
    private static final BagFactory bf = BagFactory.getInstance();

    private int numRecs;
    private boolean diversityAdjust;

    /**
     * For a single user, takes a bag of weighted user-item links of items that the user has seen
     * and a set of candidate items to recommended to the user and output at most the top N unique 
     * number of recommendations that the user hasn't seen before.
     * 
     * Input Schema:
     *  ( user_item_signals: { (user: chararray, item:chararray, weight:float) },
     *    user_recs_tmp:     { (user: chararray, item:chararray, weight:float, reason:chararray,
     *                        user_link:float, item_link:float) } )
     * Output Schema: { (user:chararray, item:chararray, weight:float, reason:chararray,
     *                   user_link:float, item_link:float, diversity_adj_weight:float, rank:int) }
     *
     * @param numRecs: Number of recommendations to return.
     * @param diversityAdjust: If True: Try to pick item recommendations with different @reason values.
     */
    public RefineUserItemRecs(String numRecs, String diversityAdjust) {
        this.numRecs = Integer.parseInt(numRecs);
        this.diversityAdjust = Boolean.parseBoolean(diversityAdjust);
    }

    public Schema outputSchema(Schema input) {
        try {
            ArrayList<FieldSchema> tupleFields = new ArrayList<FieldSchema>(8);
            tupleFields.add(new Schema.FieldSchema("user", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("item", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("weight", DataType.FLOAT));
            tupleFields.add(new Schema.FieldSchema("reason", DataType.CHARARRAY));
            tupleFields.add(new Schema.FieldSchema("user_link", DataType.FLOAT));
            tupleFields.add(new Schema.FieldSchema("item_link", DataType.FLOAT));
            tupleFields.add(new Schema.FieldSchema("diversity_adj_weight", DataType.FLOAT));
            tupleFields.add(new Schema.FieldSchema("rank", DataType.INTEGER));

            return new Schema(
                new Schema.FieldSchema("ui_recs",
                    new Schema(
                        new Schema.FieldSchema(null,
                            new Schema(tupleFields),
                        DataType.TUPLE)),
                DataType.BAG)
            );
        } catch (FrontendException e) {
            throw new RuntimeException(e);
        }
    }

    public DataBag exec(Tuple input) {
        try {
            DataBag uiWeights = (DataBag) input.get(0);
            DataBag uiRecsTemp = (DataBag) input.get(1);

            //Create a set of items that the user has already seen.
            THashSet<String> seenBefore = new THashSet<String>();
            for (Tuple t : uiWeights) {
                    String item = (String) t.get(1);
                    seenBefore.add(item);
            }

            TObjectFloatHashMap<String> seenInRecs = new TObjectFloatHashMap<String>();
            HashMap<String, Tuple> candidates = new HashMap<String, Tuple>();

            //Go through the possible recommendations for the user and collect
            //the unseen recommendations.  Any item recommended for more than one
            //reason will only be saved once with the 'best' reason.
            for (Tuple t : uiRecsTemp) {
                Object item = t.get(1);
                // skip items already seen by user
                if (seenBefore.contains(item)) { continue; }

                // only take the best rec for an given item
                // out of any duplicate occurrences
                float weight = (Float) t.get(2);
                String sitem = (String) item;
                if ((!seenInRecs.containsKey(sitem))
                        || (weight > seenInRecs.get(sitem))) {
                        seenInRecs.put(sitem, weight);
                        candidates.put(sitem, t);
                }
            }

            DataBag outputBag = bf.newDefaultBag();
            List<Tuple> candValues = new ArrayList<Tuple>(candidates.values());

            if (diversityAdjust) {
                applyDiversityAdjustment(candValues);
            } else {
                // If we're not adjusting for diversity, the adjusted weight is just the current weight.
                for (Tuple t : candValues) {
                    t.append(t.get(2));
                }
            }
            
            //field @ pos 6 is diversity_adj_weight
            Collections.sort(candValues, getTupleComparator(6));
            
            for (int i = 0; i < Math.min(numRecs, candValues.size()); i++) {
                Tuple t = candValues.get(i);
                t.append(i + 1);
                outputBag.add(t);
            }

            return outputBag;
        } catch (ExecException e) {
            throw new RuntimeException(e);
        }
    }


    private void applyDiversityAdjustment(List<Tuple> candValues)
            throws ExecException {
        HashMap<String, ArrayList<Tuple>> reasons = new HashMap<String, ArrayList<Tuple>>();
        
        // Group candidate recommendations by reason.
        for (Tuple t : candValues) {
            String reason = (String) t.get(3);
            if(reason!= null){
            
                if (reasons.get(reason) == null) {
                    ArrayList<Tuple> reasonList = new ArrayList<Tuple>();
                    reasonList.add(t);
                    reasons.put(reason, reasonList);
                } else {
                    ArrayList<Tuple> reasonList = reasons.get(reason);
                    reasonList.add(t);
                }
            }
        }

        // Calculate a diversity adjusted weight for every item.  
        // diversity_adj_weight = weight / (reason_rank + 1)
        for (String s : reasons.keySet()) {
            ArrayList<Tuple> li = reasons.get(s);
            Collections.sort(li, getTupleComparator(2));

            for (int i = 0; i < li.size(); i++) {
                Tuple t = li.get(i);
                t.append(((Float) t.get(2)) / (i + 2));
            }
        }
    }
    
    private Comparator<Tuple> getTupleComparator(final int fieldToSort) {
        return new Comparator<Tuple>() {
            public int compare(Tuple u, Tuple v) {
                try {
                    Float uWeight = (Float) u.get(fieldToSort);
                    Float vWeight = (Float) v.get(fieldToSort);
                    return vWeight.compareTo(uWeight);
                } catch (ExecException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
