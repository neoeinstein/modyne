//! An example of using modyne to implement a simple single-table
//! database for an e-commerce application as described in Chapter 19
//! of the DynamoDB book.

use core::fmt;
use std::collections::HashMap;

use aliri_braid::braid;
use modyne::{
    expr, keys, model::TransactWrite, projections, read_projection, Aggregate, Entity, EntityExt,
    Error, Item, Projection, QueryInput, QueryInputExt, Table,
};
use svix_ksuid::{Ksuid, KsuidLike};

pub struct App {
    table_name: std::sync::Arc<str>,
    client: aws_sdk_dynamodb::Client,
}

impl App {
    pub fn new(client: aws_sdk_dynamodb::Client) -> Self {
        Self::new_with_table(client, "EcommerceTable")
    }

    pub fn new_with_table(client: aws_sdk_dynamodb::Client, table_name: &str) -> Self {
        Self {
            table_name: std::sync::Arc::from(table_name),
            client,
        }
    }
}

impl Table for App {
    type PrimaryKey = keys::Primary;
    type IndexKeys = keys::Gsi1;

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }
}

impl App {
    pub async fn create_customer(&self, input: Customer) -> Result<(), Error> {
        let email_entity = CustomerEmail {
            email: input.email.clone(),
            user_name: input.user_name.clone(),
        };

        let _result = TransactWrite::new()
            .operation(input.create())
            .operation(email_entity.create())
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn upsert_address(
        &self,
        user_name: &UserNameRef,
        address_type: &str,
        input: Address,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #address.#address_type = :address")
            .name("#address", "address")
            .name("#address_type", address_type)
            .value(":address", input);

        Customer::update(user_name)
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn get_customer_orders_page(
        &self,
        user_name: &UserNameRef,
        next: Option<Item>,
        limit: Option<u32>,
    ) -> Result<(CustomerOrders, Option<Item>), Error> {
        let query_input = CustomerOrdersQuery { user_name };

        let mut customer_orders = CustomerOrders::default();

        let result = query_input
            .query()
            .set_exclusive_start_key(next)
            .set_limit(limit)
            .execute(self)
            .await?;

        customer_orders.reduce(result.items.unwrap_or_default())?;

        Ok((customer_orders, result.last_evaluated_key))
    }

    pub async fn save_order(&self, order: Order, items: Vec<OrderItem>) -> Result<(), Error> {
        let mut builder = TransactWrite::new().operation(order.create());

        for item in items {
            builder = builder.operation(item.create());
        }

        let _result = builder.execute(self).await?;

        Ok(())
    }

    pub async fn update_order_status(
        &self,
        user_name: &UserNameRef,
        order_id: OrderId,
        status: OrderStatus,
    ) -> Result<(), Error> {
        let key = OrderKeyInput {
            user_name,
            order_id,
        };

        let expression = expr::Update::new("SET #status = :status")
            .name("#status", "status")
            .value(":status", status);

        Order::update(key)
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn get_order(&self, order_id: OrderId) -> Result<OrderWithItems, Error> {
        let query_input = OrderWithItemsQuery { order_id };

        let mut order = OrderWithItems::default();
        let mut next = None;

        loop {
            let result = query_input
                .query()
                .set_exclusive_start_key(next)
                .execute(self)
                .await?;

            order.reduce(result.items.unwrap_or_default())?;

            let Some(last_evaluated_key) = result.last_evaluated_key else {
                break;
            };

            next = Some(last_evaluated_key);
        }

        Ok(order)
    }
}

#[braid(serde)]
pub struct UserName;

#[braid(serde)]
pub struct UserEmail;

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Customer {
    pub user_name: UserName,
    pub name: String,
    pub email: UserEmail,
    pub addresses: HashMap<String, Address>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct Address {
    pub street: String,
    pub city: String,
    pub state: String,
}

impl Entity for Customer {
    type KeyInput<'a> = &'a UserNameRef;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("CUSTOMER#{}", input);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        Self::primary_key(&self.user_name).into()
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
struct CustomerEmail {
    user_name: UserName,
    email: UserEmail,
}

impl Entity for CustomerEmail {
    type KeyInput<'a> = &'a UserEmailRef;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("CUSTOMEREMAIL#{}", input);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        Self::primary_key(&self.email).into()
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct OrderId(Ksuid);

impl OrderId {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self(Ksuid::new(None, None))
    }
}

impl fmt::Display for OrderId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for OrderId {
    type Err = svix_ksuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ksuid::from_str(s).map(Self)
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Order {
    pub user_name: UserName,
    pub order_id: OrderId,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
    pub number_of_items: u32,
    pub amount: f32,
    pub status: OrderStatus,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OrderStatus {
    Accepted,
    Canceled,
    Shipped,
    Delivered,
}

pub struct OrderKeyInput<'a> {
    user_name: &'a UserNameRef,
    order_id: OrderId,
}

impl Entity for Order {
    type KeyInput<'a> = OrderKeyInput<'a>;
    type Table = App;
    type IndexKeys = keys::Gsi1;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("CUSTOMER#{}", input.user_name),
            range: format!("#ORDER#{}", input.order_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(OrderKeyInput {
                user_name: &self.user_name,
                order_id: self.order_id,
            }),
            indexes: keys::Gsi1 {
                hash: format!("ORDER#{}", self.order_id),
                range: format!("ORDER#{}", self.order_id),
            },
        }
    }
}

#[braid(serde)]
pub struct ItemId;

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct OrderItem {
    pub order_id: Ksuid,
    pub item_id: ItemId,
    pub description: String,
    pub price: f32,
}

#[derive(Debug)]
pub struct OrderItemKeyInput<'a> {
    order_id: Ksuid,
    item_id: &'a ItemIdRef,
}

impl Entity for OrderItem {
    type KeyInput<'a> = OrderItemKeyInput<'a>;
    type Table = App;
    type IndexKeys = keys::Gsi1;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("ORDER#{}", input.order_id),
            range: format!("ORDER#{}#ITEM#{}", input.order_id, input.item_id),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(OrderItemKeyInput {
                order_id: self.order_id,
                item_id: &self.item_id,
            }),
            indexes: keys::Gsi1 {
                hash: format!("ORDER#{}", self.order_id),
                range: format!("ITEM#{}", self.item_id),
            },
        }
    }
}

/// A projection of customer data that does not include address information.
#[derive(Debug, Projection, serde::Serialize, serde::Deserialize)]
#[entity(Customer)]
pub struct CustomerHeader {
    pub user_name: UserName,
    pub name: String,
    pub email: UserEmail,
}

#[derive(Debug, Default)]
pub struct CustomerOrders {
    pub orders: Vec<Order>,
    pub customer: Option<CustomerHeader>,
}

pub struct CustomerOrdersQuery<'a> {
    user_name: &'a UserNameRef,
}

impl QueryInput for CustomerOrdersQuery<'_> {
    type Index = keys::Primary;
    type Aggregate = CustomerOrders;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("CUSTOMER#{}", self.user_name))
    }
}

projections! {
    pub enum CustomerOrdersEntities {
        Order,
        CustomerHeader,
    }
}

impl Aggregate for CustomerOrders {
    type Projections = CustomerOrdersEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::Order(order) => self.orders.push(order),
            Self::Projections::CustomerHeader(header) => self.customer = Some(header),
        }

        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct OrderWithItems {
    pub order: Option<Order>,
    pub items: Vec<OrderItem>,
}

pub struct OrderWithItemsQuery {
    pub order_id: OrderId,
}

impl QueryInput for OrderWithItemsQuery {
    type Index = keys::Gsi1;
    type Aggregate = OrderWithItems;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        expr::KeyCondition::in_partition(format!("ORDER#{}", self.order_id))
    }
}

projections! {
    pub enum OrderWithItemsEntities {
        Order,
        OrderItem,
    }
}

impl Aggregate for OrderWithItems {
    type Projections = OrderWithItemsEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::Order(order) => self.order = Some(order),
            Self::Projections::OrderItem(item) => self.items.push(item),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use aws_sdk_dynamodb::types::AttributeValue;

    use super::*;

    #[test]
    fn verify_user_orders_entities_projection_expression() {
        assert_eq!(
        <CustomerOrdersEntities as modyne::ProjectionSet>::projection_expression(),
        Some(expr::StaticProjection {
            expression: "user_name,order_id,created_at,number_of_items,amount,#prj_000,#prj_001,email,entity_type",
            names: &[
                ("#prj_000", "status"),
                ("#prj_001", "name"),
            ],
        })
    );
    }

    #[test]
    fn verify_order_with_items_entities_projection_expression() {
        assert_eq!(
        <OrderWithItemsEntities as modyne::ProjectionSet>::projection_expression(),
        Some(expr::StaticProjection {
            expression: "user_name,order_id,created_at,number_of_items,amount,#prj_000,item_id,description,price,entity_type",
            names: &[
                ("#prj_000", "status"),
            ],
        }),
    );
    }

    #[test]
    fn verify_order_entity_full_item_serializes_as_expected() {
        let order_id = "1VrgXBQ0VCshuQUnh1HrDIHQNwY".parse().unwrap();
        let order = Order {
            user_name: UserName::from_static("alexdebrie"),
            order_id,
            created_at: time::OffsetDateTime::from_unix_timestamp(1578016664).unwrap(),
            number_of_items: 7,
            status: OrderStatus::Shipped,
            amount: 67.43,
        };

        let item = order.into_item();

        assert_eq!(item["PK"].as_s().unwrap(), "CUSTOMER#alexdebrie");
        assert_eq!(
            item["SK"].as_s().unwrap(),
            "#ORDER#1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(
            item["GSI1PK"].as_s().unwrap(),
            "ORDER#1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(
            item["GSI1SK"].as_s().unwrap(),
            "ORDER#1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(item["entity_type"].as_s().unwrap(), "order");
        assert_eq!(item["user_name"].as_s().unwrap(), "alexdebrie");
        assert_eq!(
            item["order_id"].as_s().unwrap(),
            "1VrgXBQ0VCshuQUnh1HrDIHQNwY"
        );
        assert_eq!(item["created_at"].as_s().unwrap(), "2020-01-03T01:57:44Z");
        assert_eq!(item["number_of_items"].as_n().unwrap(), "7");
        assert_eq!(item["status"].as_s().unwrap(), "SHIPPED");
        assert_eq!(item["amount"].as_n().unwrap(), "67.43");
        assert_eq!(item.len(), 11);
    }

    #[test]
    fn verify_customer_orders_entity_hydrates_as_expected() {
        #[allow(non_snake_case)]
        fn Str(s: &str) -> AttributeValue {
            AttributeValue::S(s.to_string())
        }

        #[allow(non_snake_case)]
        fn Num(s: &str) -> AttributeValue {
            AttributeValue::N(s.to_string())
        }

        let items = [
            [
                ("entity_type", Str("customer")),
                ("user_name", Str("alexdebrie")),
                ("name", Str("Alex DeBrie")),
                ("email", Str("alexdebrie1@gmail.com")),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect::<Item>(),
            [
                ("entity_type", Str("order")),
                ("user_name", Str("alexdebrie")),
                ("order_id", Str("1VwVAvJk1GvBFfpTAjm0KG7Cg9d")),
                ("created_at", Str("2020-01-04T18:53:24Z")),
                ("number_of_items", Num("2")),
                ("status", Str("CANCELED")),
                ("amount", Num("12.43")),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
            [
                ("entity_type", Str("order")),
                ("user_name", Str("alexdebrie")),
                ("order_id", Str("1VrgXBQ0VCshuQUnh1HrDIHQNwY")),
                ("created_at", Str("2020-01-03T01:57:44Z")),
                ("number_of_items", Num("7")),
                ("status", Str("SHIPPED")),
                ("amount", Num("67.43")),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        ];

        let mut customer_orders = CustomerOrders::default();

        for item in items {
            customer_orders.merge(item).unwrap();
        }

        assert!(customer_orders.customer.is_some());
        assert_eq!(customer_orders.orders.len(), 2);
    }
}
