#![doc = include_str!("../README.md")]

use core::fmt;
use std::{collections::VecDeque, num::NonZeroU32};

use aliri_braid::braid;
use aws_sdk_dynamodb::operation::scan::ScanOutput;
use modyne::{
    expr,
    keys::{self, IndexKey},
    model::{Scan, ScanSegment, TransactWrite},
    projections, read_projection, Aggregate, AttributeValue, Entity, EntityExt, EntityTypeNameRef,
    Error, Item, ProjectionExt, QueryInput, QueryInputExt, ScanInput, Table,
};
use serde_dynamo::string_set::StringSet;
use svix_ksuid::{Ksuid, KsuidLike};
use time::format_description::well_known::Rfc3339;

#[derive(Clone, Debug)]
pub struct App {
    table_name: std::sync::Arc<str>,
    client: aws_sdk_dynamodb::Client,
}

impl App {
    pub fn new(client: aws_sdk_dynamodb::Client) -> Self {
        Self::new_with_table(client, "BigTimeDeals")
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
    type IndexKeys = (keys::Gsi1, keys::Gsi2, keys::Gsi3, UserIndex);

    fn table_name(&self) -> &str {
        &self.table_name
    }

    fn client(&self) -> &aws_sdk_dynamodb::Client {
        &self.client
    }

    /// For demonstration purposes, this example uses a non-standard
    /// attribute value for storing the entity type
    ///
    /// In general, you don't need to specify this function, and use of the provided default
    /// is recommended.
    fn deserialize_entity_type(
        attr: &AttributeValue,
    ) -> Result<&EntityTypeNameRef, modyne::MalformedEntityTypeError> {
        let values = attr.as_ss().map_err(|_| {
            modyne::MalformedEntityTypeError::Custom("expected a string set".into())
        })?;
        let value = values
            .first()
            .expect("a DynamoDB string set always has at least one element");
        Ok(EntityTypeNameRef::from_str(value.as_str()))
    }

    /// For demonstration purposes, this example uses a non-standard
    /// attribute value for storing the entity type
    ///
    /// In general, you don't need to specify this function, and use of the provided default
    /// is recommended.
    fn serialize_entity_type(entity_type: &EntityTypeNameRef) -> AttributeValue {
        AttributeValue::Ss(vec![entity_type.to_string()])
    }
}

impl App {
    pub async fn create_deal(&self, deal: Deal) -> Result<(), Error> {
        deal.create().execute(self).await?;
        Ok(())
    }

    pub async fn create_brand(&self, brand: Brand) -> Result<(), Error> {
        let expression = expr::Update::new("ADD #brands :brands SET #entity_type = :entity_type")
            .name("#brands", "brands")
            .value(":brands", StringSet(vec![&brand.brand_name]))
            .name("#entity_type", "entity_type")
            .value(":entity_type", <Brands as modyne::EntityDef>::ENTITY_TYPE);
        let update = Brands::update(()).expression(expression);

        TransactWrite::new()
            .operation(update)
            .operation(brand.create())
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn create_category(&self, category: Category) -> Result<(), Error> {
        category.create().execute(self).await?;
        Ok(())
    }

    pub async fn set_featured_deals_front_page(
        &self,
        featured_deals: Vec<FeaturedDeal>,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #featured_deals = :featured_deals")
            .name("#featured_deals", "featured_deals")
            .value(":featured_deals", featured_deals);
        FrontPage::update(())
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn set_featured_deals_for_category(
        &self,
        category: &CategoryNameRef,
        featured_deals: Vec<FeaturedDeal>,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #featured_deals = :featured_deals")
            .name("#featured_deals", "featured_deals")
            .value(":featured_deals", featured_deals);
        Category::update(category)
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn set_featured_deals_editors_choice(
        &self,
        featured_deals: Vec<FeaturedDeal>,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #featured_deals = :featured_deals")
            .name("#featured_deals", "featured_deals")
            .value(":featured_deals", featured_deals);
        EditorsChoice::update(())
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn get_front_page(&self) -> Result<FrontPage, Error> {
        let result = FrontPage::get(()).execute(self).await?;
        FrontPage::from_item(result.item.unwrap_or_default())
    }

    pub async fn get_category(
        &self,
        category: &CategoryNameRef,
    ) -> Result<Option<Category>, Error> {
        let result = Category::get(category).execute(self).await?;
        result.item.map(Category::from_item).transpose()
    }

    pub async fn get_editors_choice_page(&self) -> Result<EditorsChoice, Error> {
        let result = EditorsChoice::get(()).execute(self).await?;
        EditorsChoice::from_item(result.item.unwrap_or_default())
    }

    pub async fn get_deal(&self, deal_id: DealId) -> Result<Option<Deal>, Error> {
        let resp = Deal::get(deal_id).execute(self).await?;

        resp.item.map(Deal::from_item).transpose()
    }

    pub async fn get_deals_by_date(
        &self,
        date: time::Date,
        last_seen: Option<DealId>,
    ) -> Result<Vec<Deal>, Error> {
        const DEFAULT_LIMIT: u32 = 25;
        let mut limit = DEFAULT_LIMIT;
        let mut query_input = DealsByDateQuery { date, last_seen };

        let mut agg = Vec::with_capacity(DEFAULT_LIMIT as usize);

        for _ in 0..5 {
            let result = query_input.query().limit(limit).execute(self).await?;

            agg.reduce(result.items.unwrap_or_default())?;

            query_input.date = query_input.date.previous_day().unwrap();
            limit = limit.saturating_sub(result.count as u32);
            if limit == 0 {
                break;
            }
        }

        Ok(agg)
    }

    pub async fn get_brand_deals_by_date(
        &self,
        brand: &BrandNameRef,
        date: time::Date,
        last_seen: Option<DealId>,
    ) -> Result<Vec<Deal>, Error> {
        const DEFAULT_LIMIT: u32 = 25;
        let mut limit = DEFAULT_LIMIT;
        let mut query_input = BrandDealsByDateQuery {
            brand,
            date,
            last_seen,
        };

        let mut agg = Vec::with_capacity(DEFAULT_LIMIT as usize);

        for _ in 0..5 {
            let result = query_input.query().limit(limit).execute(self).await?;

            agg.reduce(result.items.unwrap_or_default())?;

            query_input.date = query_input.date.previous_day().unwrap();
            limit = limit.saturating_sub(result.count as u32);
            if limit == 0 {
                break;
            }
        }

        Ok(agg)
    }

    pub async fn get_category_deals_by_date(
        &self,
        category: &CategoryNameRef,
        date: time::Date,
        last_seen: Option<DealId>,
    ) -> Result<Vec<Deal>, Error> {
        const DEFAULT_LIMIT: u32 = 25;
        let mut limit = DEFAULT_LIMIT;
        let mut query_input = CategoryDealsByDateQuery {
            category,
            date,
            last_seen,
        };

        let mut agg = Vec::with_capacity(DEFAULT_LIMIT as usize);

        for _ in 0..5 {
            let result = query_input.query().limit(limit).execute(self).await?;

            agg.reduce(result.items.unwrap_or_default())?;

            query_input.date = query_input.date.previous_day().unwrap();
            limit = limit.saturating_sub(result.count as u32);
            if limit == 0 {
                break;
            }
        }

        Ok(agg)
    }

    pub async fn get_all_brands(&self) -> Result<Brands, Error> {
        let resp = Brands::get(()).execute(self).await?;

        Ok(resp
            .item
            .map(Brands::from_item)
            .transpose()?
            .unwrap_or(Brands { brands: Vec::new() }))
    }

    pub async fn put_brand_like(
        &self,
        brand_name: BrandName,
        user_name: UserName,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #likes = #likes + :incr")
            .name("#likes", "likes")
            .value(":incr", 1);
        let condition = expr::Condition::new("attribute_exists(#PK)")
            .name("#PK", Brand::KEY_DEFINITION.hash_key);

        let update = Brand::update(&brand_name)
            .expression(expression)
            .condition(condition);

        TransactWrite::new()
            .operation(update)
            .operation(
                BrandLike {
                    brand_name,
                    user_name,
                }
                .create(),
            )
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn put_brand_watch(
        &self,
        brand_name: BrandName,
        user_name: UserName,
    ) -> Result<(), Error> {
        BrandWatch {
            brand_name,
            user_name,
        }
        .create()
        .execute(self)
        .await?;
        Ok(())
    }

    pub async fn put_category_like(
        &self,
        category_name: CategoryName,
        user_name: UserName,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #likes = #likes + :incr")
            .name("#likes", "likes")
            .value(":incr", 1);
        let condition = expr::Condition::new("attribute_exists(#PK)")
            .name("#PK", Category::KEY_DEFINITION.hash_key);

        let update = Category::update(&category_name)
            .expression(expression)
            .condition(condition);

        TransactWrite::new()
            .operation(update)
            .operation(
                CategoryLike {
                    category_name,
                    user_name,
                }
                .create(),
            )
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn put_category_watch(
        &self,
        category_name: CategoryName,
        user_name: UserName,
    ) -> Result<(), Error> {
        CategoryWatch {
            category_name,
            user_name,
        }
        .create()
        .execute(self)
        .await?;
        Ok(())
    }

    pub async fn get_watchers_by_brand(
        &self,
        brand_name: &BrandNameRef,
        last_seen: Option<&UserNameRef>,
    ) -> Result<Watchers, Error> {
        let query_input = WatchersByBrandQuery {
            brand_name,
            last_seen,
        };

        let mut agg = Watchers::default();

        let result = query_input.query().execute(self).await?;

        agg.reduce(result.items.unwrap_or_default())?;

        Ok(agg)
    }

    pub async fn get_watchers_by_category(
        &self,
        category_name: &CategoryNameRef,
        last_seen: Option<&UserNameRef>,
    ) -> Result<Watchers, Error> {
        let query_input = WatchersByCategoryQuery {
            category_name,
            last_seen,
        };

        let mut agg = Watchers::default();

        let result = query_input.query().execute(self).await?;

        agg.reduce(result.items.unwrap_or_default())?;

        Ok(agg)
    }

    pub async fn create_message(
        &self,
        to: &UserNameRef,
        subject: String,
        body: String,
    ) -> Result<MessageId, Error> {
        let now = time::OffsetDateTime::now_utc();
        let message_id = MessageId::new(now);
        let message = Message {
            user_name: to.to_owned(),
            message_id,
            subject,
            body,
            unread: true,
            created_at: now,
        };

        message.create().execute(self).await?;

        Ok(message_id)
    }

    pub async fn mark_message_read(
        &self,
        user_name: &UserNameRef,
        message_id: MessageId,
    ) -> Result<(), Error> {
        let expression = expr::Update::new("SET #unread = :unread, REMOVE #GSIPK, #GSISK")
            .name("#unread", "unread")
            .name("#GSIPK", keys::Gsi1::INDEX_DEFINITION.hash_key())
            .name("#GSISK", keys::Gsi1::INDEX_DEFINITION.range_key().unwrap())
            .value(":unread", false);

        Message::update((user_name, message_id))
            .expression(expression)
            .execute(self)
            .await?;

        Ok(())
    }

    pub async fn get_all_messages(
        &self,
        user_name: &UserNameRef,
        last_seen: Option<MessageId>,
    ) -> Result<Vec<Message>, Error> {
        let query_input = AllMessagesByUserQuery {
            user_name,
            last_seen,
        };

        let mut agg = Vec::default();

        let result = query_input.query().execute(self).await?;

        agg.reduce(result.items.unwrap_or_default())?;

        Ok(agg)
    }

    pub async fn get_unread_messages(
        &self,
        user_name: &UserNameRef,
        last_seen: Option<MessageId>,
    ) -> Result<Vec<Message>, Error> {
        let query_input = UnreadMessagesByUserQuery {
            user_name,
            last_seen,
        };

        let mut agg = Vec::default();

        let result = query_input.query().execute(self).await?;

        agg.reduce(result.items.unwrap_or_default())?;

        Ok(agg)
    }

    pub async fn create_user(&self, user_name: UserName) -> Result<(), Error> {
        User {
            user_name,
            created_at: time::OffsetDateTime::now_utc(),
        }
        .create()
        .execute(self)
        .await?;

        Ok(())
    }

    pub fn get_all_users(&self) -> UsersStream {
        self.get_all_users_parallel(0, NonZeroU32::new(1).unwrap())
    }

    pub fn get_all_users_parallel(&self, segment: u32, total_segments: NonZeroU32) -> UsersStream {
        let template = Scan::<UserIndex>::new().segment(ScanSegment {
            segment: segment as i32,
            total_segments: total_segments.get() as i32,
        });

        UsersStream::new(self.clone(), template)
    }
}

pin_project_lite::pin_project! {
    pub struct UsersStream {
        #[pin]
        inner: std::pin::Pin<Box<dyn futures::stream::Stream<Item = Result<User, Error>> + Send>>
    }
}

type StreamOutput = Result<User, Error>;
type StreamState = Option<(Option<Item>, VecDeque<Item>)>;

impl UsersStream {
    fn new(table: App, template: Scan<UserIndex>) -> Self {
        let stream = futures::stream::unfold(None, move |state| {
            Self::advance_users_stream(table.clone(), template.clone(), state)
        });

        Self {
            inner: Box::pin(stream),
        }
    }

    async fn advance_users_stream(
        table: App,
        template: Scan<UserIndex>,
        state: StreamState,
    ) -> Option<(StreamOutput, StreamState)> {
        if let Some((last, mut items)) = state {
            if let Some(item) = items.pop_front() {
                let parsed = User::from_item(item).map_err(Error::from);
                return Some((parsed, Some((last, items))));
            }

            let result = template
                .exclusive_start_key(last.clone()?)
                .execute(&table)
                .await;

            match result {
                Ok(output) => Self::handle_returned_items(output),
                Err(err) => Some((Err(err.into()), Some((last, items)))),
            }
        } else {
            let result = template.execute(&table).await;

            match result {
                Ok(output) => Self::handle_returned_items(output),
                Err(err) => Some((Err(err.into()), None)),
            }
        }
    }

    fn handle_returned_items(output: ScanOutput) -> Option<(StreamOutput, StreamState)> {
        let mut items = VecDeque::from(output.items.unwrap_or_default());
        let next = output.last_evaluated_key;

        let item = items.pop_front()?;
        let parsed = User::from_item(item).map_err(Error::from);

        Some((parsed, Some((next, items))))
    }
}

impl futures::stream::Stream for UsersStream {
    type Item = Result<User, Error>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx)
    }
}

struct UserIndexScan;

impl ScanInput for UserIndexScan {
    type Index = UserIndex;

    fn projection_expression() -> Option<expr::StaticProjection> {
        modyne::once_projection_expression!(User)
    }
}

#[braid(serde)]
pub struct BrandName;

#[braid(serde)]
pub struct CategoryName;

#[braid(serde)]
pub struct UserName;

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct DealId(Ksuid);

impl DealId {
    #[allow(clippy::new_without_default)]
    pub fn new(now: time::OffsetDateTime) -> Self {
        Self(Ksuid::new(Some(now), None))
    }
}

impl fmt::Display for DealId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for DealId {
    type Err = svix_ksuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ksuid::from_str(s).map(Self)
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Deal {
    pub deal_id: DealId,
    pub title: String,
    pub link: String,
    pub price: f64,
    pub category: CategoryName,
    pub brand: BrandName,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
}

impl Entity for Deal {
    type KeyInput<'a> = DealId;
    type Table = App;
    type IndexKeys = (keys::Gsi1, keys::Gsi2, keys::Gsi3);

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("DEAL#{}", input);
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        let date = format_as_date(self.created_at.date());
        keys::FullKey {
            primary: Self::primary_key(self.deal_id),
            indexes: (
                keys::Gsi1 {
                    hash: format!("DEALS#{}", date),
                    range: format!("DEAL#{}", self.deal_id),
                },
                keys::Gsi2 {
                    hash: format!("BRAND#{}#{}", self.brand, date).to_ascii_uppercase(),
                    range: format!("DEAL#{}", self.deal_id),
                },
                keys::Gsi3 {
                    hash: format!("CATEGORY#{}#{}", self.category, date).to_ascii_uppercase(),
                    range: format!("DEAL#{}", self.deal_id),
                },
            ),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Brand {
    pub brand_name: BrandName,
    pub brand_logo_url: String,
    pub likes: u32,
}

impl Entity for Brand {
    type KeyInput<'a> = &'a BrandNameRef;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("BRAND#{}", input).to_ascii_uppercase();
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(&self.brand_name),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct BrandLike {
    pub brand_name: BrandName,
    pub user_name: UserName,
}

impl Entity for BrandLike {
    type KeyInput<'a> = (&'a BrandNameRef, &'a UserNameRef);
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!(
            "BRANDLIKE#{}#{}",
            input.0.as_str().to_ascii_uppercase(),
            input.1
        );
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key((&self.brand_name, &self.user_name)),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct BrandWatch {
    pub brand_name: BrandName,
    pub user_name: UserName,
}

impl Entity for BrandWatch {
    type KeyInput<'a> = (&'a BrandNameRef, &'a UserNameRef);
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("BRANDWATCH#{}", input.0).to_ascii_uppercase(),
            range: format!("USER#{}", input.1),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key((&self.brand_name, &self.user_name)),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Brands {
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        with = "serde_dynamo::string_set"
    )]
    pub brands: Vec<BrandName>,
}

impl Entity for Brands {
    type KeyInput<'a> = ();
    type Table = App;
    type IndexKeys = ();

    fn primary_key(_: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: "BRANDS".to_string(),
            range: "BRANDS".to_string(),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(()),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Category {
    pub category_name: CategoryName,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub featured_deals: Vec<FeaturedDeal>,
    pub likes: u32,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct FeaturedDeal {
    pub deal_title: String,
}

impl Entity for Category {
    type KeyInput<'a> = &'a CategoryNameRef;
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!("CATEGORY#{}", input).to_ascii_uppercase();
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(&self.category_name),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct CategoryLike {
    pub category_name: CategoryName,
    pub user_name: UserName,
}

impl Entity for CategoryLike {
    type KeyInput<'a> = (&'a CategoryNameRef, &'a UserNameRef);
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        let common = format!(
            "CATEGORYLIKE#{}#{}",
            input.0.as_str().to_ascii_uppercase(),
            input.1
        );
        keys::Primary {
            hash: common.clone(),
            range: common,
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key((&self.category_name, &self.user_name)),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct CategoryWatch {
    pub category_name: CategoryName,
    pub user_name: UserName,
}

impl Entity for CategoryWatch {
    type KeyInput<'a> = (&'a CategoryNameRef, &'a UserNameRef);
    type Table = App;
    type IndexKeys = ();

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("CATEGORYWATCH#{}", input.0).to_ascii_uppercase(),
            range: format!("USER#{}", input.1),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key((&self.category_name, &self.user_name)),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct FrontPage {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub featured_deals: Vec<FeaturedDeal>,
}

impl Entity for FrontPage {
    type KeyInput<'a> = ();
    type Table = App;
    type IndexKeys = ();

    fn primary_key(_: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: "FRONTPAGE".to_string(),
            range: "FRONTPAGE".to_string(),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(()),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct EditorsChoice {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub featured_deals: Vec<FeaturedDeal>,
}

impl Entity for EditorsChoice {
    type KeyInput<'a> = ();
    type Table = App;
    type IndexKeys = ();

    fn primary_key(_: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: "EDITORSCHOICE".to_string(),
            range: "EDITORSCHOICE".to_string(),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(()),
            indexes: (),
        }
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct User {
    pub user_name: UserName,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub struct UserIndex {
    #[serde(rename = "user_index")]
    pub hash: String,
}

impl keys::IndexKey for UserIndex {
    const INDEX_DEFINITION: keys::SecondaryIndexDefinition = keys::GlobalSecondaryIndexDefinition {
        index_name: "user_index",
        hash_key: "user_index",
        range_key: None,
    }
    .into_index();
}

impl Entity for User {
    type KeyInput<'a> = &'a UserNameRef;
    type Table = App;
    type IndexKeys = UserIndex;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("USER#{}", input),
            range: format!("USER#{}", input),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        keys::FullKey {
            primary: Self::primary_key(&self.user_name),
            indexes: UserIndex {
                hash: format!("USER#{}", self.user_name),
            },
        }
    }
}

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct MessageId(Ksuid);

impl MessageId {
    #[allow(clippy::new_without_default)]
    pub fn new(now: time::OffsetDateTime) -> Self {
        Self(Ksuid::new(Some(now), None))
    }
}

impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl std::str::FromStr for MessageId {
    type Err = svix_ksuid::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ksuid::from_str(s).map(Self)
    }
}

#[derive(Debug, modyne::EntityDef, serde::Serialize, serde::Deserialize)]
pub struct Message {
    pub user_name: UserName,
    pub message_id: MessageId,
    pub subject: String,
    pub body: String,
    pub unread: bool,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: time::OffsetDateTime,
}

impl Entity for Message {
    type KeyInput<'a> = (&'a UserNameRef, MessageId);
    type Table = App;
    type IndexKeys = Option<keys::Gsi1>;

    fn primary_key(input: Self::KeyInput<'_>) -> keys::Primary {
        keys::Primary {
            hash: format!("MESSAGES#{}", input.0),
            range: format!("MESSAGE#{}", input.1),
        }
    }

    fn full_key(&self) -> keys::FullKey<keys::Primary, Self::IndexKeys> {
        let index = self.unread.then(|| keys::Gsi1 {
            hash: format!("MESSAGES#{}", self.user_name),
            range: format!("MESSAGE#{}", self.message_id),
        });

        keys::FullKey {
            primary: Self::primary_key((&self.user_name, self.message_id)),
            indexes: index,
        }
    }
}

pub struct AllMessagesByUserQuery<'a> {
    pub user_name: &'a UserNameRef,
    pub last_seen: Option<MessageId>,
}

impl QueryInput for AllMessagesByUserQuery<'_> {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Primary;
    type Aggregate = Vec<Message>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let partition = format!("MESSAGES#{}", self.user_name);
        let bound = format!(
            "MESSAGE#{}",
            self.last_seen.map(|id| id.to_string()).unwrap_or_default()
        );
        expr::KeyCondition::in_partition(partition).less_than(bound)
    }
}

pub struct UnreadMessagesByUserQuery<'a> {
    pub user_name: &'a UserNameRef,
    pub last_seen: Option<MessageId>,
}

impl QueryInput for UnreadMessagesByUserQuery<'_> {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Gsi1;
    type Aggregate = Vec<Message>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let partition = format!("MESSAGES#{}", self.user_name);
        let bound = format!(
            "MESSAGE#{}",
            self.last_seen.map(|id| id.to_string()).unwrap_or_default()
        );
        expr::KeyCondition::in_partition(partition).less_than(bound)
    }
}

#[derive(Debug)]
pub struct WatchersByBrandQuery<'a> {
    pub brand_name: &'a BrandNameRef,
    pub last_seen: Option<&'a UserNameRef>,
}

impl QueryInput for WatchersByBrandQuery<'_> {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Primary;
    type Aggregate = Watchers;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let partition = format!("BRANDWATCH#{}", self.brand_name);
        let bound = self
            .last_seen
            .map(|id| format!("USER#{}", id))
            .unwrap_or_default();
        expr::KeyCondition::in_partition(partition).greater_than(bound)
    }
}

#[derive(Debug)]
pub struct WatchersByCategoryQuery<'a> {
    pub category_name: &'a CategoryNameRef,
    pub last_seen: Option<&'a UserNameRef>,
}

impl QueryInput for WatchersByCategoryQuery<'_> {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Primary;
    type Aggregate = Watchers;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let partition = format!("CATEGORYWATCH#{}", self.category_name);
        let bound = self
            .last_seen
            .map(|id| format!("USER#{}", id))
            .unwrap_or_default();
        expr::KeyCondition::in_partition(partition).greater_than(bound)
    }
}

#[derive(Debug, Default)]
pub struct Watchers {
    pub watchers: Vec<UserName>,
}

projections! {
    pub enum WatchersByBrandEntities {
        BrandWatch,
    }
}

impl Aggregate for Watchers {
    type Projections = WatchersByBrandEntities;

    fn merge(&mut self, item: Item) -> Result<(), Error> {
        match read_projection!(item)? {
            Self::Projections::BrandWatch(watcher) => self.watchers.push(watcher.user_name),
        }

        Ok(())
    }
}

#[derive(Debug)]
pub struct DealsByDateQuery {
    pub date: time::Date,
    pub last_seen: Option<DealId>,
}

impl QueryInput for DealsByDateQuery {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Gsi1;
    type Aggregate = Vec<Deal>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let date = format_as_date(self.date);
        let partition = format!("DEALS#{}", date);
        let bound = self
            .last_seen
            .map(|id| format!("DEAL#{}", id))
            .unwrap_or_else(|| "DEAL$".to_string());
        expr::KeyCondition::in_partition(partition).less_than(bound)
    }
}

#[derive(Debug)]
pub struct BrandDealsByDateQuery<'a> {
    pub brand: &'a BrandNameRef,
    pub date: time::Date,
    pub last_seen: Option<DealId>,
}

impl QueryInput for BrandDealsByDateQuery<'_> {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Gsi2;
    type Aggregate = Vec<Deal>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let date = self.date.format(&Rfc3339).unwrap();
        let partition = format!("BRAND#{}#{}", self.brand, date).to_ascii_uppercase();
        let bound = self
            .last_seen
            .map(|id| format!("DEAL#{}", id))
            .unwrap_or_else(|| "DEAL$".to_string());
        expr::KeyCondition::in_partition(partition).less_than(bound)
    }
}

#[derive(Debug)]
pub struct CategoryDealsByDateQuery<'a> {
    pub category: &'a CategoryNameRef,
    pub date: time::Date,
    pub last_seen: Option<DealId>,
}

impl QueryInput for CategoryDealsByDateQuery<'_> {
    const SCAN_INDEX_FORWARD: bool = false;

    type Index = keys::Gsi3;
    type Aggregate = Vec<Deal>;

    fn key_condition(&self) -> expr::KeyCondition<Self::Index> {
        let date = self.date.format(&Rfc3339).unwrap();
        let partition = format!("CATEGORY#{}#{}", self.category, date).to_ascii_uppercase();
        let bound = self
            .last_seen
            .map(|id| format!("DEAL#{}", id))
            .unwrap_or_else(|| "DEAL$".to_string());
        expr::KeyCondition::in_partition(partition).less_than(bound)
    }
}

fn format_as_date(time: time::Date) -> String {
    #[cfg(not(feature = "once_cell"))]
    static FORMAT: std::sync::OnceLock<Vec<time::format_description::FormatItem<'static>>> =
        std::sync::OnceLock::new();

    #[cfg(feature = "once_cell")]
    static FORMAT: once_cell::sync::OnceCell<Vec<time::format_description::FormatItem<'static>>> =
        once_cell::sync::OnceCell::new();

    let format = FORMAT.get_or_init(|| {
        time::format_description::parse_borrowed::<2>("[year]-[month]-[day]").unwrap()
    });

    time.format(format).unwrap()
}

#[cfg(test)]
mod tests {
    use modyne::keys::PrimaryKey;

    use super::*;

    #[test]
    fn message_read_serializes_as_expected() {
        let now = time::OffsetDateTime::now_utc();
        let message = Message {
            user_name: "user".into(),
            message_id: MessageId::new(now),
            subject: "subject".into(),
            body: "body".into(),
            created_at: now,
            unread: false,
        };

        let item = dbg!(message.into_item());

        assert!(item.contains_key(keys::Primary::PRIMARY_KEY_DEFINITION.hash_key));
        assert!(item.contains_key(keys::Primary::PRIMARY_KEY_DEFINITION.range_key.unwrap()));
        assert!(!item.contains_key(keys::Gsi1::INDEX_DEFINITION.hash_key()));
        assert!(!item.contains_key(keys::Gsi1::INDEX_DEFINITION.range_key().unwrap()));
    }

    #[test]
    fn message_unread_serializes_as_expected() {
        let now = time::OffsetDateTime::now_utc();
        let message = Message {
            user_name: "user".into(),
            message_id: MessageId::new(now),
            subject: "subject".into(),
            body: "body".into(),
            created_at: now,
            unread: true,
        };

        let item = dbg!(message.into_item());

        assert!(item.contains_key(keys::Primary::PRIMARY_KEY_DEFINITION.hash_key));
        assert!(item.contains_key(keys::Primary::PRIMARY_KEY_DEFINITION.range_key.unwrap()));
        assert!(item.contains_key(keys::Gsi1::INDEX_DEFINITION.hash_key()));
        assert!(item.contains_key(keys::Gsi1::INDEX_DEFINITION.range_key().unwrap()));
        assert_eq!(
            item.get(keys::Primary::PRIMARY_KEY_DEFINITION.hash_key),
            item.get(keys::Gsi1::INDEX_DEFINITION.hash_key())
        );
        assert_eq!(
            item.get(keys::Primary::PRIMARY_KEY_DEFINITION.range_key.unwrap()),
            item.get(keys::Gsi1::INDEX_DEFINITION.range_key().unwrap())
        );
    }
}
