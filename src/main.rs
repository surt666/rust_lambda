use std::error::Error;

use lambda_runtime::{error::HandlerError, lambda, Context};
use log::{self, error};
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeValue, DynamoDb, DynamoDbClient, GetItemError, GetItemInput, QueryError, QueryInput,
};
use serde_derive::{Deserialize, Serialize};
use simple_error::bail;
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
    GetItem {a: String, b: String},
//    GetRelations(String)
}

#[derive(Deserialize)]
struct ActionEvent {
    action: Actions
}

#[derive(Serialize)]
struct CustomOutput {
    message: String,
    item: HashMap<String, AttributeValue>,
    items: Vec<HashMap<String, AttributeValue>>
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

async fn get_item(
    client: &DynamoDbClient,
    key: HashMap<String, AttributeValue>,
    table: &str,
) -> Result<HashMap<String, AttributeValue>, RusotoError<GetItemError>> {
    let get_item_input = GetItemInput {
        key: key,
        table_name: table.to_string(),
        ..Default::default()
    };
    match client.get_item(get_item_input).await {
        Ok(output) => match output.item {
            Some(item) => Ok(item),
            None => Ok(HashMap::new()),
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
) -> Result<Vec<HashMap<String, AttributeValue>>, RusotoError<QueryError>> {
    let query_input = QueryInput {
        key_condition_expression: key_exp,
        expression_attribute_values: exp_attr_vals,
        table_name: table.to_string(),
        index_name: index,
        ..Default::default()
    };
    match client.query(query_input).await {
        Ok(output) => match output.items {
            Some(items) => Ok(items),
            None => Ok(Vec::new()),
        },
        Err(error) => Err(error),
    }
}

#[tokio::main]
async fn my_handler(e: ActionEvent, _c: Context) -> Result<CustomOutput, HandlerError> {
    let client = DynamoDbClient::new(Region::default());
    let mut key: HashMap<String, AttributeValue> = HashMap::new();
    set_kv(&mut key, "pk".to_string(), "c4c".to_string());
    set_kv(&mut key, "sk".to_string(), "c4c".to_string());
    let item = get_item(&client, key, "relations").await.unwrap();
    println!("Item {:#?}", item);
    let mut key_exp: HashMap<String, AttributeValue> = HashMap::new();
    set_kv(&mut key_exp, ":itemtype".to_string(), "dataset".to_string());
    let items = query_items(
        &client,
        Some("itemtype = :itemtype".to_string()),
        Some(key_exp),
        "relations",
        Some("itemtype-index".to_string()),
    )
    .await
    .unwrap();
    println!("Items {:#?}", items);
    match e.action {
	Actions::GetDatasets => Ok(CustomOutput {
	    items: items,
	    message: "".to_string(),
	    item: HashMap::new()
	}),
	Actions::GetItem {a, b} => Ok(CustomOutput {
	    item: item,
	    message: a.to_string(),
	    items: vec![HashMap::new()]
	})
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Warn)?;
    lambda!(my_handler);
    Ok(())
}
