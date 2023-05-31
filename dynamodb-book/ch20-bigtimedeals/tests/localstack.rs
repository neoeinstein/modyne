use std::num::NonZeroU32;

use dynamodb_book_ch20_bigtimedeals::{
    App, Brand, BrandName, CategoryName, Deal, DealId, UserName,
};
use modyne::TestTableExt;

#[test_log::test(tokio::test)]
#[ignore = "this test requires a local DynamoDB instance running on localhost:4566 and may be \
            slow"]
async fn localstack_only_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use futures::stream::TryStreamExt;

    let config = aws_config::from_env()
        .endpoint_url("http://localhost:4566")
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "static",
        ))
        .load()
        .await;
    let client = aws_sdk_dynamodb::Client::new(&config);
    let app = App::new(client);

    let _ = app.delete_table().send().await;

    let _create_table = app.create_table().send().await?;

    let mut handles = tokio::task::JoinSet::new();
    for i in 0..100 {
        let app = app.clone();
        handles.spawn(async move { app.create_user(UserName::from(format!("mtest_{i}"))).await });
    }
    while handles
        .join_next()
        .await
        .transpose()?
        .transpose()?
        .is_some()
    {}

    let mut handles = tokio::task::JoinSet::new();
    for i in 0..10 {
        let app = app.clone();
        handles.spawn(async move {
            let mut users_stream = app.get_all_users_parallel(i, NonZeroU32::new(10).unwrap());
            while let Some(result) = users_stream.try_next().await? {
                println!("from {}: {:?}", i, result);
            }
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        });
    }
    while handles
        .join_next()
        .await
        .transpose()?
        .transpose()?
        .is_some()
    {}

    app.create_brand(Brand {
        brand_name: BrandName::from("mtest_brand"),
        brand_logo_url: "https://example.com/logo.png".to_string(),
        likes: 0,
    })
    .await?;

    app.put_brand_like(BrandName::from("mtest_brand"), UserName::from("mtest_0"))
        .await?;

    assert!(app
        .create_brand(Brand {
            brand_name: BrandName::from("mtest_brand"),
            brand_logo_url: "https://example.com/logo.png".to_string(),
            likes: 0,
        })
        .await
        .is_err());

    let now = time::OffsetDateTime::now_utc();
    app.create_deal(Deal {
        deal_id: DealId::new(now),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("orange"),
        brand: BrandName::from("mtest_brand"),
        created_at: now,
    })
    .await?;

    let then = now.saturating_sub(time::Duration::days(1));
    app.create_deal(Deal {
        deal_id: DealId::new(then),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("orange"),
        brand: BrandName::from("mtest_brand"),
        created_at: then,
    })
    .await?;

    let thenthen = then.saturating_sub(time::Duration::days(1));
    app.create_deal(Deal {
        deal_id: DealId::new(thenthen),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("blue"),
        brand: BrandName::from("mtest_brand"),
        created_at: thenthen,
    })
    .await?;

    let thenthen = thenthen.saturating_sub(time::Duration::days(1));
    app.create_deal(Deal {
        deal_id: DealId::new(thenthen),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("green"),
        brand: BrandName::from("mtest_brand"),
        created_at: thenthen,
    })
    .await?;

    let thenthen = thenthen.saturating_sub(time::Duration::days(1));
    app.create_deal(Deal {
        deal_id: DealId::new(thenthen),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("orange"),
        brand: BrandName::from("mtest_brand"),
        created_at: thenthen,
    })
    .await?;

    app.create_deal(Deal {
        deal_id: DealId::new(thenthen),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("green"),
        brand: BrandName::from("mtest_brand"),
        created_at: thenthen,
    })
    .await?;

    let thenthen = thenthen.saturating_sub(time::Duration::days(1));
    app.create_deal(Deal {
        deal_id: DealId::new(thenthen),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("black"),
        brand: BrandName::from("mtest_brand"),
        created_at: thenthen,
    })
    .await?;

    let thenthen = thenthen.saturating_sub(time::Duration::days(1));
    app.create_deal(Deal {
        deal_id: DealId::new(thenthen),
        title: "mtest_deal".to_string(),
        link: "mtest_deal".to_string(),
        price: 19.99,
        category: CategoryName::from_static("blacker"),
        brand: BrandName::from("mtest_brand"),
        created_at: thenthen,
    })
    .await?;

    dbg!(app.get_deals_by_date(now.date(), None).await?);

    Ok(())
}
