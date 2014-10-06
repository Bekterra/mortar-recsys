SET dynamodb.throughput.write.percent 1.0;

item_item_recs = LOAD '$OUTPUT_PATH/item_item_recs'
    USING PigStorage()
    AS (from_id:chararray, to_id:chararray, weight:float, raw_weight:float, rank:int);

user_item_recs = LOAD '$OUTPUT_PATH/user_item_recs'
    USING PigStorage()
    AS (from_id:chararray, to_id:chararray, weight:float, reason_item:chararray, user_reason_item_weight:float, item_reason_item_weight:float, rank:int);

-- STORE the item_item_recs into dynamo
STORE item_item_recs
 INTO '$OUTPUT_PATH/unused-ii-table-data'
USING com.mortardata.pig.storage.DynamoDBStorage('$II_TABLE', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY');

-- STORE the user_item_recs into dynamo
STORE user_item_recs
 INTO '$OUTPUT_PATH/unused-ui-table-data'
USING com.mortardata.pig.storage.DynamoDBStorage('$UI_TABLE', '$AWS_ACCESS_KEY_ID', '$AWS_SECRET_ACCESS_KEY');
