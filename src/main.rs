use async_trait::async_trait;
use lambda_runtime::{error::HandlerError, lambda, Context};
use log::{self, error};
use rusoto_core::{Region, RusotoError};
use rusoto_dynamodb::{
    AttributeValue, DynamoDb, DynamoDbClient, GetItemError, GetItemInput, QueryError, QueryInput,
};
use serde::{Deserialize, Serialize};
use std::error::Error;
//use serde_dynamodb::Error as DynError;
//use simple_error::bail;
use simple_logger;
use std::collections::HashMap;

const RELATIONS_TABLE: &str = "relations";

#[derive(Deserialize)]
enum Actions {
    GetDatasets,
    GetItem { pk: String, sk: String },
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
    itemtype: String,
    created: Option<u64>,
}

trait DdbKey {
    fn pk(&self) -> String;
    fn sk(&self) -> String;
}

trait Itemtype {
    fn itemtype(&self) -> String;
}

impl DdbKey for Dataset {
    fn pk(&self) -> String {
        self.pk.clone()
    }
    fn sk(&self) -> String {
        self.sk.clone()
    }
}

impl Itemtype for Dataset {
    fn itemtype(&self) -> String {
        self.itemtype.clone()
    }
}

impl Default for Dataset {
    fn default() -> Self {
        Dataset {
            pk: "".to_string(),
            sk: "".to_string(),
            itemtype: "".to_string(),
	    created: Some(0),
        }
    }
}

fn set_kv(
    item: &mut HashMap<String, AttributeValue>, key: String, val: String,
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

async fn query_items<'a, T: Deserialize<'a>>(
    client: &DynamoDbClient, key_exp: Option<String>, exp_attr_vals: Option<DdbMap>, table: &str,
    index: Option<String>,
) -> Result<Vec<T>, RusotoError<QueryError>> {
    let query_input = QueryInput {
        key_condition_expression: key_exp,
        expression_attribute_values: exp_attr_vals,
        table_name: table.to_string(),
        index_name: index,
        ..Default::default()
    };
    let datasets: Vec<T> = client
        .query(query_input)
        .await
        .unwrap()
        .items
        .unwrap_or_else(|| vec![])
        .into_iter()
        .map(|item| serde_dynamodb::from_hashmap(item).unwrap())
        .collect();
    Ok(datasets)
}

type DdbMap = HashMap<String, AttributeValue>;

#[async_trait]
impl Ddb<'_> for Dataset {}

#[async_trait]
trait Ddb<'a>: Deserialize<'a> + Default + DdbKey + Itemtype {
    async fn get_item(
        &'a self, client: &DynamoDbClient, table: &str,
    ) -> Result<Self, RusotoError<GetItemError>> {
        let mut key: DdbMap = HashMap::new();
        set_kv(&mut key, "pk".to_string(), self.pk());
        set_kv(&mut key, "sk".to_string(), self.sk());
        let get_item_input = GetItemInput {
            key: key,
            table_name: table.to_string(),
            ..Default::default()
        };
        let res = client.get_item(get_item_input).await.unwrap().item;
        match res {
            Some(item) => Ok(serde_dynamodb::from_hashmap(item).unwrap()),
            None => Ok(Self::default()),
        }
    }
    async fn query_by_itemtype(&'a self, client: &DynamoDbClient, table: &str) -> Vec<Self> {
        let mut key_exp: DdbMap = HashMap::new();
        set_kv(&mut key_exp, ":itemtype".to_string(), self.itemtype());
        query_items(
            &client,
            Some("itemtype = :itemtype".to_string()),
            Some(key_exp),
            table,
            Some("itemtype-index".to_string()),
        )
        .await
        .unwrap()
    }
}

async fn get_item<'a, T: Deserialize<'a> + Default>(client: &DynamoDbClient, table: &str, key: DdbMap) -> T {
    let get_item_input = GetItemInput {
        key: key,
        table_name: table.to_string(),
        ..Default::default()
    };
    let res = client.get_item(get_item_input)
	.await
	.unwrap()
	.item
	.unwrap();
    serde_dynamodb::from_hashmap(res).unwrap()
}

async fn query_by_itemtype<'a, T: Deserialize<'a>>(client: &DynamoDbClient, table: &str, itemtype: &str) -> Vec<T> {
    let mut key_exp: DdbMap = HashMap::new();
    set_kv(&mut key_exp, ":itemtype".to_string(), itemtype.to_string());
    query_items(
        &client,
        Some("itemtype = :itemtype".to_string()),
        Some(key_exp),
        table,
        Some("itemtype-index".to_string()),
    )
        .await
        .unwrap()
}

#[tokio::main]
async fn my_handler(e: ActionEvent, _c: Context) -> Result<CustomOutput, HandlerError> {
    let client = DynamoDbClient::new(Region::default());
    match e.action {
        Actions::GetDatasets => {
            let ds = Dataset {
                itemtype: "dataset".to_string(),
                ..Default::default()
            };
            let datasets = ds.query_by_itemtype(&client, RELATIONS_TABLE).await;
            println!("Items {:#?}", datasets);
            Ok(CustomOutput {
                datasets: datasets,
                message: "".to_string(),
                dataset: Dataset::default()
            })
        }
        Actions::GetItem { pk, sk } => {
            let ds = Dataset {
                pk: pk.to_string(),
                sk: sk.to_string(),
                ..Default::default()
            };
            let dataset = ds.get_item(&client, RELATIONS_TABLE).await.unwrap();
            println!("Item {:#?}", dataset);
            Ok(CustomOutput {
                dataset: dataset,
                message: pk.to_string(),
                datasets: vec![],
            })
        }
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    simple_logger::init_with_level(log::Level::Warn)?;
    lambda!(my_handler);
    Ok(())
}
