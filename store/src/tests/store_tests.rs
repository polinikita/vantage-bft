// Copyright(C) Facebook, Inc. and its affiliates.
use super::*;
use std::fs;

#[tokio::test]
async fn create_store() {
    // Create new store.
    let path = ".db_test_create_store";
    let _ = fs::remove_dir_all(path);
    let store = Store::new(path);
    assert!(store.is_ok());
}

#[tokio::test]
async fn read_write_value() {
    // Create new store.
    let path = ".db_test_read_write_value";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Write value to the store.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let value = vec![4u8, 5u8, 6u8, 7u8];
    store.write(key.clone(), value.clone()).await;

    // Read value.
    let result = store.read(key).await;
    assert!(result.is_ok());
    let read_value = result.unwrap();
    assert!(read_value.is_some());
    assert_eq!(read_value.unwrap(), value);
}

#[tokio::test]
async fn read_unknown_key() {
    // Create new store.
    let path = ".db_test_read_unknown_key";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Try to read unknown key.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let result = store.read(key).await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn read_many_preserves_order_and_gaps() {
    let path = ".db_test_read_many";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Two present keys, interleaved with two absent ones.
    let k_a = vec![1u8];
    let k_b = vec![2u8];
    let k_missing_1 = vec![3u8];
    let k_missing_2 = vec![4u8];
    store.write(k_a.clone(), vec![10u8]).await;
    store.write(k_b.clone(), vec![20u8]).await;

    // Requested order deliberately differs from write order, and repeats a key: the
    // reply must line up index-for-index with the request.
    let got = store
        .read_many(vec![
            k_missing_1,
            k_b.clone(),
            k_missing_2,
            k_a.clone(),
            k_b.clone(),
        ])
        .await
        .unwrap();
    assert_eq!(
        got,
        vec![
            None,
            Some(vec![20u8]),
            None,
            Some(vec![10u8]),
            Some(vec![20u8]),
        ]
    );

    // Empty request short-circuits without touching the store.
    assert!(store.read_many(Vec::new()).await.unwrap().is_empty());
}

#[tokio::test]
async fn batched_writes_survive_the_flush() {
    // Writes are buffered and flushed as one batch; they must be readable both BEFORE
    // the flush (via the pending overlay) and AFTER it (from RocksDB itself).
    let path = ".db_test_batched_flush";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    let keys: Vec<Key> = (0u8..8).map(|i| vec![i]).collect();
    for (i, key) in keys.iter().enumerate() {
        store.write(key.clone(), vec![i as u8, 0xAA]).await;
    }

    // Before any tick: served from the pending overlay.
    let before = store.read_many(keys.clone()).await.unwrap();
    assert!(before.iter().all(Option::is_some), "pre-flush read failed");

    // Let at least one flush tick fire, then read again -- now out of RocksDB.
    tokio::time::sleep(Duration::from_millis(FLUSH_INTERVAL_MS * 3)).await;
    let after = store.read_many(keys).await.unwrap();
    assert_eq!(before, after, "values changed across the flush boundary");
    assert_eq!(after[3], Some(vec![3u8, 0xAA]));
}

#[tokio::test]
async fn read_notify() {
    // Create new store.
    let path = ".db_test_read_notify";
    let _ = fs::remove_dir_all(path);
    let mut store = Store::new(path).unwrap();

    // Try to read a key that does not yet exist. Then write a value
    // for that key and check that notify read returns the result.
    let key = vec![0u8, 1u8, 2u8, 3u8];
    let value = vec![4u8, 5u8, 6u8, 7u8];

    // Try to read a missing value.
    let mut store_copy = store.clone();
    let key_copy = key.clone();
    let value_copy = value.clone();
    let handle = tokio::spawn(async move {
        match store_copy.notify_read(key_copy).await {
            Ok(v) => assert_eq!(v, value_copy),
            _ => panic!("Failed to read from store"),
        }
    });

    // Write the missing value and ensure the handle terminates correctly.
    store.write(key, value).await;
    assert!(handle.await.is_ok());
}
