use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use cid::Cid;
use libp2p::identity::Keypair;
use neverust_core::{api, ArchivistTree, BlockStore, BoTgConfig, BoTgProtocol, Manifest, Metrics};
use std::sync::{Arc, RwLock};
use tower::util::ServiceExt;

fn test_app() -> (axum::Router, Arc<BlockStore>) {
    let block_store = Arc::new(BlockStore::new());
    let metrics = Metrics::new();
    let peer_id = "12D3KooWParityTest".to_string();
    let botg = Arc::new(BoTgProtocol::new(BoTgConfig::default()));
    let keypair = Arc::new(Keypair::generate_ed25519());
    let listen_addrs = Arc::new(RwLock::new(vec!["/ip4/127.0.0.1/tcp/8070"
        .parse()
        .expect("valid multiaddr")]));

    let app = api::create_router(
        block_store.clone(),
        metrics,
        peer_id,
        botg,
        keypair,
        listen_addrs,
    );

    (app, block_store)
}

#[tokio::test]
async fn archivist_endpoint_parity_smoke_test() {
    let (app, _) = test_app();
    let payload = b"parity-check-payload".to_vec();

    let upload = Request::builder()
        .method("POST")
        .uri("/api/archivist/v1/data")
        .header("content-type", "application/octet-stream")
        .body(Body::from(payload.clone()))
        .expect("valid upload request");
    let upload_resp = app.clone().oneshot(upload).await.expect("upload response");
    assert_eq!(upload_resp.status(), StatusCode::OK);
    let manifest_cid = String::from_utf8(
        to_bytes(upload_resp.into_body(), usize::MAX)
            .await
            .expect("upload body")
            .to_vec(),
    )
    .expect("manifest cid text");
    assert!(manifest_cid.starts_with('z'));

    let list = Request::builder()
        .uri("/api/archivist/v1/data")
        .body(Body::empty())
        .expect("valid list request");
    let list_resp = app.clone().oneshot(list).await.expect("list response");
    assert_eq!(list_resp.status(), StatusCode::OK);
    let list_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(list_resp.into_body(), usize::MAX)
            .await
            .expect("list body"),
    )
    .expect("list json");
    let items = list_json["content"]
        .as_array()
        .expect("content array present");
    assert!(items.iter().any(|item| item["cid"] == manifest_cid));

    let local_get = Request::builder()
        .uri(format!("/api/archivist/v1/data/{}", manifest_cid))
        .body(Body::empty())
        .expect("valid local get request");
    let local_get_resp = app
        .clone()
        .oneshot(local_get)
        .await
        .expect("local get response");
    assert_eq!(local_get_resp.status(), StatusCode::OK);
    let local_bytes = to_bytes(local_get_resp.into_body(), usize::MAX)
        .await
        .expect("local get body");
    assert_eq!(local_bytes.as_ref(), payload.as_slice());

    let stream_get = Request::builder()
        .uri(format!(
            "/api/archivist/v1/data/{}/network/stream",
            manifest_cid
        ))
        .body(Body::empty())
        .expect("valid stream request");
    let stream_resp = app
        .clone()
        .oneshot(stream_get)
        .await
        .expect("stream response");
    assert_eq!(stream_resp.status(), StatusCode::OK);
    let stream_bytes = to_bytes(stream_resp.into_body(), usize::MAX)
        .await
        .expect("stream body");
    assert_eq!(stream_bytes.as_ref(), payload.as_slice());

    let manifest_get = Request::builder()
        .uri(format!(
            "/api/archivist/v1/data/{}/network/manifest",
            manifest_cid
        ))
        .body(Body::empty())
        .expect("valid network manifest request");
    let manifest_get_resp = app
        .clone()
        .oneshot(manifest_get)
        .await
        .expect("network manifest response");
    assert_eq!(manifest_get_resp.status(), StatusCode::OK);
    let manifest_json: serde_json::Value = serde_json::from_slice(
        &to_bytes(manifest_get_resp.into_body(), usize::MAX)
            .await
            .expect("network manifest body"),
    )
    .expect("network manifest json");
    assert_eq!(manifest_json["cid"], manifest_cid);

    let space = Request::builder()
        .uri("/api/archivist/v1/space")
        .body(Body::empty())
        .expect("valid space request");
    let space_resp = app.clone().oneshot(space).await.expect("space response");
    assert_eq!(space_resp.status(), StatusCode::OK);

    let peer_id = Request::builder()
        .uri("/api/archivist/v1/peer-id")
        .body(Body::empty())
        .expect("valid peer-id request");
    let peer_id_resp = app
        .clone()
        .oneshot(peer_id)
        .await
        .expect("peer-id response");
    assert_eq!(peer_id_resp.status(), StatusCode::OK);
    let peer_id_body = to_bytes(peer_id_resp.into_body(), usize::MAX)
        .await
        .expect("peer-id body");

    let peerid = Request::builder()
        .uri("/api/archivist/v1/peerid")
        .body(Body::empty())
        .expect("valid peerid request");
    let peerid_resp = app.clone().oneshot(peerid).await.expect("peerid response");
    assert_eq!(peerid_resp.status(), StatusCode::OK);
    let peerid_body = to_bytes(peerid_resp.into_body(), usize::MAX)
        .await
        .expect("peerid body");
    assert_eq!(peer_id_body, peerid_body);

    let connect = Request::builder()
        .uri("/api/archivist/v1/connect/12D3KooWMock")
        .body(Body::empty())
        .expect("valid connect request");
    let connect_resp = app
        .clone()
        .oneshot(connect)
        .await
        .expect("connect response");
    assert_eq!(connect_resp.status(), StatusCode::NOT_IMPLEMENTED);

    let sales = Request::builder()
        .uri("/api/archivist/v1/sales/slots")
        .body(Body::empty())
        .expect("valid sales request");
    let sales_resp = app.clone().oneshot(sales).await.expect("sales response");
    assert_eq!(sales_resp.status(), StatusCode::SERVICE_UNAVAILABLE);

    let delete = Request::builder()
        .method("DELETE")
        .uri(format!("/api/archivist/v1/data/{}", manifest_cid))
        .body(Body::empty())
        .expect("valid delete request");
    let delete_resp = app.clone().oneshot(delete).await.expect("delete response");
    assert_eq!(delete_resp.status(), StatusCode::NO_CONTENT);

    let local_get_missing = Request::builder()
        .uri(format!("/api/archivist/v1/data/{}", manifest_cid))
        .body(Body::empty())
        .expect("valid local get request");
    let local_get_missing_resp = app
        .clone()
        .oneshot(local_get_missing)
        .await
        .expect("local missing response");
    assert_eq!(local_get_missing_resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn local_download_does_not_fetch_missing_blocks_from_network() {
    let (app, block_store) = test_app();
    let payload = vec![42u8; 200_000];

    let upload = Request::builder()
        .method("POST")
        .uri("/api/archivist/v1/data")
        .header("content-type", "application/octet-stream")
        .body(Body::from(payload))
        .expect("valid upload request");
    let upload_resp = app.clone().oneshot(upload).await.expect("upload response");
    assert_eq!(upload_resp.status(), StatusCode::OK);

    let manifest_cid_str = String::from_utf8(
        to_bytes(upload_resp.into_body(), usize::MAX)
            .await
            .expect("upload body")
            .to_vec(),
    )
    .expect("manifest cid text")
    .trim()
    .to_string();
    let manifest_cid: Cid = manifest_cid_str.parse().expect("valid manifest cid");

    let manifest_block = block_store
        .get(&manifest_cid)
        .await
        .expect("manifest should exist");
    let manifest = Manifest::from_block(&manifest_block).expect("valid manifest");
    let metadata_cid: Cid = manifest
        .filename
        .expect("manifest filename")
        .strip_prefix("metadata:")
        .expect("metadata prefix")
        .parse()
        .expect("metadata cid");

    let metadata_block = block_store
        .get(&metadata_cid)
        .await
        .expect("metadata should exist");
    let block_cids = ArchivistTree::deserialize_block_list(&metadata_block.data)
        .expect("valid metadata block list");
    assert!(!block_cids.is_empty());

    block_store
        .delete(&block_cids[0])
        .await
        .expect("delete one block");

    let local_get = Request::builder()
        .uri(format!("/api/archivist/v1/data/{}", manifest_cid_str))
        .body(Body::empty())
        .expect("valid local get request");
    let local_get_resp = app
        .clone()
        .oneshot(local_get)
        .await
        .expect("local get response");
    assert_eq!(local_get_resp.status(), StatusCode::NOT_FOUND);
}
