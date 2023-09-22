use aws_sdk_dynamodb::types::TimeToLiveSpecification;
use dynamodb_book_ch18_sessionstore::{App, Session, Username, UsernameRef};
use modyne::{
    expr,
    model::{BatchGet, BatchWrite},
    types::Expiry,
    EntityExt, ProjectionExt, Table, TestTableExt,
};

#[test_log::test(tokio::test)]
#[ignore = "this test requires a local DynamoDB instance running on localhost:4566 and may be \
            slow"]
async fn localstack_only_test() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    app.client()
        .update_time_to_live()
        .table_name(app.table_name())
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("ttl")
                .enabled(true)
                .build(),
        )
        .send()
        .await?;

    let mut handles = tokio::task::JoinSet::new();
    for i in 0..100 {
        let app = app.clone();
        handles.spawn(async move {
            app.create_session(Session {
                session_token: uuid::Uuid::new_v4(),
                username: Username::from(format!("mtest_{}", i % 13)),
                created_at: time::OffsetDateTime::now_utc(),
                expires_at: time::OffsetDateTime::now_utc(),
                ttl: Expiry::from(time::OffsetDateTime::now_utc()),
            })
            .await
        });
    }
    while handles
        .join_next()
        .await
        .transpose()?
        .transpose()?
        .is_some()
    {}

    app.delete_user_sessions(UsernameRef::from_static("mtest_6"))
        .await?;

    let session_token = uuid::Uuid::new_v4();

    let now = time::OffsetDateTime::now_utc();
    let expires = time::OffsetDateTime::now_utc() + time::Duration::hours(5);
    app.create_session(Session {
        session_token,
        username: Username::from("session_test"),
        created_at: now,
        expires_at: expires,
        ttl: expires.into(),
    })
    .await?;

    let session = app.get_session_with_now(session_token, now).await?.unwrap();
    assert_eq!(session.username, UsernameRef::from_static("session_test"));

    let session = app.get_session_with_now(session_token, expires).await?;
    assert!(session.is_none());

    Ok(())
}

#[test_log::test(tokio::test)]
#[ignore = "this test requires a local DynamoDB instance running on localhost:4566 and may be \
            slow"]
async fn batch_put_get_delete() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config = aws_config::from_env()
        .endpoint_url("http://localhost:4566")
        .credentials_provider(aws_credential_types::Credentials::new(
            "test", "test", None, None, "static",
        ))
        .load()
        .await;
    let client = aws_sdk_dynamodb::Client::new(&config);
    let app = App::new_with_table(client, "SessionStore_BatchTest");

    let _ = app.delete_table().send().await;

    let _create_table = app.create_table().send().await?;

    app.client()
        .update_time_to_live()
        .table_name(app.table_name())
        .time_to_live_specification(
            TimeToLiveSpecification::builder()
                .attribute_name("ttl")
                .enabled(true)
                .build(),
        )
        .send()
        .await?;

    const WRITE_BATCH_SIZE: usize = 25;
    const READ_BATCH_SIZE: usize = 100;
    let mut i = 0;
    let operations = std::iter::from_fn(move || {
        i += 1;
        let put = Session {
            session_token: uuid::Uuid::new_v4(),
            username: Username::from(format!("mtest_{}", i % 7)),
            created_at: time::OffsetDateTime::now_utc(),
            expires_at: time::OffsetDateTime::now_utc(),
            ttl: Expiry::from(time::OffsetDateTime::now_utc()),
        };
        Some(put)
    })
    .take(READ_BATCH_SIZE + 29)
    .collect::<Vec<_>>();

    for b in operations.chunks(WRITE_BATCH_SIZE) {
        let mut batch = BatchWrite::new();
        for op in b {
            batch = batch.operation(op.clone().put());
        }
        let result = batch.execute(&app).await?;

        assert!(result.unprocessed_items.unwrap_or_default().is_empty());
    }

    for b in operations.chunks(READ_BATCH_SIZE) {
        let mut batch = BatchGet::new();
        for op in b {
            batch = batch.operation(Session::get(op.session_token));
        }
        let result = batch.execute(&app).await?;

        assert_eq!(result.responses().map(|r| r.len()).unwrap_or_default(), 1);
        assert_eq!(
            result
                .responses()
                .and_then(|r| r.get(app.table_name()))
                .map(|t| t.len())
                .unwrap_or_default(),
            b.len()
        );
        assert!(result.unprocessed_keys.unwrap_or_default().is_empty());
    }

    for b in operations.chunks(WRITE_BATCH_SIZE) {
        let mut batch = BatchWrite::new();
        for op in b {
            batch = batch.operation(Session::delete(op.session_token));
        }
        let result = batch.execute(&app).await?;

        assert!(result.unprocessed_items.unwrap_or_default().is_empty());
    }

    let uuid = uuid::Uuid::new_v4();
    Session {
        session_token: uuid,
        username: Username::from("mtest"),
        created_at: time::OffsetDateTime::now_utc(),
        expires_at: time::OffsetDateTime::now_utc(),
        ttl: Expiry::from(time::OffsetDateTime::now_utc()),
    }
    .put()
    .execute(&app)
    .await?;

    let modyne_user = UsernameRef::from_static("modyne");
    Session::update(uuid)
        .expression(
            expr::Update::new("SET #username = :username")
                .name("#username", "username")
                .value(":username", modyne_user),
        )
        .execute(&app)
        .await?;

    let result = Session::get(uuid).execute(&app).await?;
    let session = Session::from_item(result.item.unwrap_or_default())?;
    assert_eq!(session.username, modyne_user);

    Ok(())
}
