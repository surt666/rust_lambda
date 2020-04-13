use std::error::Error;

use lambda_runtime::{error::HandlerError, lambda, Context};
use log::{self, error};
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeValue, DynamoDb, DynamoDbClient, GetItemError, GetItemInput, QueryError, QueryInput,
};
use serde::{Deserialize, Serialize};
//use serde_dynamodb::Error as DynError;
//use simple_error::bail;
use simple_logger;
use std::collections::HashMap;

/*#[derive(Deserialize)]
struct CustomEvent {
    #[serde(rename = "firstName")]
    first_name: String,
} */

#[derive(Deserialize)]
enum Actions {
    GetDatasets,
    GetItem { a: String, b: String },
    //    GetRelations(String)
}

#[derive(Deserialize)]
struct ActionEvent {
    action: Actions,
}

#[derive(Serialize)]
struct CustomOutput {
    message: String,
    dataset: Dataset,
    datasets: Vec<Dataset>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Dataset {
    pk: String,
    sk: String,
    itemtype: String
}

fn set_kv(
    item: &mut HashMap<String, AttributeValue>,
    key: String,
    val: String,
) -> &HashMap<String, AttributeValue> {
    item.insert(
        key.to_string(),
        AttributeValue {
            s: Some(val.to_string()),
            ..Default::default()
        },
    );
    item
}

async fn get_dataset(
    client: &DynamoDbClient,
    key: HashMap<String, AttributeValue>,
    table: &str,
) -> Result<Dataset, RusotoError<GetItemError>> {
    let get_item_input = GetItemInput {
        key: key,
        table_name: table.to_string(),
        ..Default::default()
    };
    match client.get_item(get_item_input).await {
        Ok(output) => match output.item {
            Some(item) => Ok(serde_dynamodb::from_hashmap(item).unwrap()),
            None => Ok(Dataset {pk: "".to_string(), sk: "".to_string(), itemtype: "".to_string()}),
        },
        Err(error) => Err(error),
    }
}

async fn query_items(
    client: &DynamoDbClient,
    key_exp: Option<String>,
    exp_attr_vals: Option<HashMap<String, AttributeValue>>,
    table: &str,
    index: Option<String>,
) -> Result<Vec<Dataset>, RusotoError<QueryError>> {
    let query_input = QueryInput {
        key_condition_expression: key_exp,
        expression_attribute_values: exp_attr_vals,
        table_name: table.to_string(),
        index_name: index,
        ..Default::default()
    };
    let datasets: Vec<Dataset> = client.query(query_input)
	.await
        .unwrap()
        .items
        .unwrap_or_else(|| vec![])
        .into_iter()
        .map(|item| serde_dynamodb::from_hashmap(item).unwrap())
        .collect();
    Ok(datasets)
/*    match client.query(query_input).await {
        Ok(output) => match output.items {
            Some(items) => Ok(items),
            None => Ok(Vec::new()),
        },
        Err(error) => Err(error),
    }*/
}

#[tokio::main]
async fn my_handler(e: ActionEvent, _c: Context) -> Result<CustomOutput, HandlerError> {
    let client = DynamoDbClient::new(Region::default());
    match e.action {
        Actions::GetDatasets => {
	    let mut key_exp: HashMap<String, AttributeValue> = HashMap::new();
	    set_kv(&mut key_exp, ":itemtype".to_string(), "dataset".to_string());
	    let datasets = query_items(
		&client,
		Some("itemtype = :itemtype".to_string()),
		Some(key_exp),
		"relations",
		Some("itemtype-index".to_string()),
	    )
		.await
		.unwrap();
	    println!("Items {:#?}", datasets);
	    Ok(CustomOutput {
		datasets: datasets,
		message: "".to_string(),
		dataset: Dataset {pk: "".to_string(), sk: "".to_string(), itemtype: "".to_string()},
            })},
        Actions::GetItem { a, b } => {
	    let mut key: HashMap<String, AttributeValue> = HashMap::new();
	    set_kv(&mut key, "pk".to_string(), a.to_string());
	    set_kv(&mut key, "sk".to_string(), b.to_string());
	    let dataset = get_dataset(&client, key, "relations").await.unwrap();
	    println!("Item {:#?}", dataset);    
	    Ok(CustomOutput {
		dataset: dataset,
		message: a.to_string(),
		datasets: vec![],
            })},
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Warn)?;
    lambda!(my_handler);
    Ok(())
}
