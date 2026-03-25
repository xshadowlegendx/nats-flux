
use salvo::Writer;
use salvo::Listener;
use opentelemetry::trace::TracerProvider;
use opentelemetry::trace::TraceContextExt;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

async fn ensure_nats_stream(nats: async_nats::Client, name: String) -> Result<(), async_nats::error::Error<async_nats::jetstream::context::CreateStreamErrorKind>> {
    let js = async_nats::jetstream::new(nats);

    let result = js.create_stream(async_nats::jetstream::stream::Config{
        name: name.clone(),
        retention: async_nats::jetstream::stream::RetentionPolicy::Limits,
        subjects: vec![format!("{}.>", name)],
        ..Default::default()
    })
        .await;

    if result.is_ok() {
        return Ok(());
    }

    Err(result.err().unwrap())
}

#[salvo::handler]
async fn save_message(
    req: &mut salvo::Request,
    resp: &mut salvo::Response,
    depot: &mut salvo::Depot,
    stream: salvo::oapi::extract::PathParam<String>
) {
    let tracing_context = opentelemetry::Context::current();
    let tracing_span = tracing_context.span();

    tracing_span.set_attribute(opentelemetry::KeyValue::new("nats_stream", stream.0.clone()));

    let subject = req.param::<&str>("subjs")
        .expect("failed to extract subjects")
        .replace("/", ".");

    let subject = format!("{}.{}", stream.0, subject);

    tracing_span.set_attribute(opentelemetry::KeyValue::new("nats_subject", subject.clone()));

    tracing::info!("message received - {}", subject);

    let nats_client = depot
        .obtain::<async_nats::Client>()
        .expect("failed to get nats client");

    let ensure_stream = ensure_nats_stream(nats_client.clone(), stream.0.clone())
        .await;
    if let Err(err) = ensure_stream {
        tracing::error!("failed to create stream - {}, {}", stream.0, err.to_string());

        resp.status_code(salvo::http::StatusCode::INTERNAL_SERVER_ERROR);
        return;
    }

    let mut headers = async_nats::HeaderMap::new();

    if let Some(id) = req.header::<&str>("x-nats-msg-id") {
        headers.append("Nats-Msg-Id", id);
        tracing_span.set_attribute(opentelemetry::KeyValue::new("nats-msg-id", id.to_string()));
    }

    let result = nats_client
        .publish_with_headers(
            subject.clone(),
            headers,
            req.payload()
                .await
                .expect("failed to extract payload")
                .clone()
        )
        .await;

    if let Err(err) = result {
        tracing::error!("failed to save message - {}, {}", &subject, err.to_string());

        resp.status_code(salvo::http::StatusCode::INTERNAL_SERVER_ERROR);
        return;
    }

    resp.status_code(salvo::http::StatusCode::NO_CONTENT);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().json())
        .with(
            tracing_subscriber::filter::EnvFilter::builder()
                .with_default_directive(tracing_subscriber::filter::LevelFilter::INFO.into())
                .parse("")
                .expect("failed to build tracing env filter")
        )
        .init();

    let nats_url = std::env::var("NATS_URL")
        .unwrap_or_else(|_| "nats://localhost:4222".to_string());

    let nats_client = async_nats::connect(nats_url)
        .await
        .expect("failed to connect to nats");

    let listener = salvo::prelude::TcpListener::new("0.0.0.0:5629")
        .bind()
        .await;

    let router = router(nats_client);

    salvo::prelude::Server::new(listener)
        .serve(router)
        .await;
}

fn router(nats_client: async_nats::Client) -> salvo::Router {
    salvo::prelude::Router::with_path("{stream}/{*subjs}")
        .hoop(salvo::affix_state::inject(nats_client))
        .hoop(salvo::otel::Metrics::new())
        .hoop(salvo::otel::Tracing::new(init_tracer_provider().tracer("app")))
        .post(save_message)
}


fn init_tracer_provider() -> opentelemetry_sdk::trace::SdkTracerProvider {
    opentelemetry::global::set_text_map_propagator(opentelemetry_sdk::propagation::TraceContextPropagator::new());
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect("failed to create exporter");

    opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .build()
}

#[cfg(test)]
mod tests {
    async fn setup() -> (salvo::prelude::Service, async_nats::Client, impl Fn(Vec<String>, Vec<String>) -> std::pin::Pin<Box<dyn Future<Output = ()>>>) {
        let nats_url = std::env::var("NATS_URL")
            .unwrap_or_else(|_| "nats://localhost:4222".to_string());

        let nats_client = async_nats::connect(nats_url)
            .await
            .expect("failed to connect to nats");

        let router = super::router(nats_client.clone());

        (
            salvo::prelude::Service::new(router),
            nats_client.clone(),
            move |streams, object_stores| {
                let js = async_nats::jetstream::new(nats_client.clone());

                Box::pin(async move {
                    for stream in streams {
                        js.delete_stream(stream)
                            .await
                            .expect("failed to clean up stream");
                    }

                    for object_store in object_stores {
                        js.delete_object_store(object_store)
                            .await
                            .expect("failed to clean up object store");
                    }
                })
            }
        )
    }

    #[tokio::test]
    async fn test_save_message_to_nats() {
        let (svc, nats_client, cleanup) = setup()
            .await;

        let status_code = salvo::test::TestClient::post("http://localhost:5629/saf/ren/ger")
            .json(&serde_json::json!({"for": "see", "sum": 2}))
            .send(&svc)
            .await
            .status_code
            .expect("failed to extract status code");

        assert_eq!(status_code, salvo::http::StatusCode::NO_CONTENT);

        let js = async_nats::jetstream::new(nats_client);

        let s = js.get_stream("saf")
            .await;

        assert!(s.is_ok());

        let msg = s
            .unwrap()
            .get_last_raw_message_by_subject("saf.ren.ger")
            .await;

        assert!(msg.is_ok());

        let payload = msg.unwrap().payload;

        let payload = std::str::from_utf8(&payload);

        assert!(payload.is_ok());

        assert_eq!(payload.unwrap(), "{\"for\":\"see\",\"sum\":2}");

        cleanup(vec!["saf".to_string()], vec![])
            .await;
    }

    #[tokio::test]
    async fn test_save_message_dedup() {
        let (svc, nats_client, cleanup) = setup()
            .await;

        let _status_code = salvo::test::TestClient::post("http://localhost:5629/foo/ren/ger")
            .json(&serde_json::json!({"for": "see", "sum": 2}))
            .add_header("x-nats-msg-id", "def123", true)
            .send(&svc)
            .await
            .status_code
            .expect("failed to extract status code");

        let _status_code = salvo::test::TestClient::post("http://localhost:5629/foo/ren/ger")
            .json(&serde_json::json!({"for": "see", "sum": 2}))
            .add_header("x-nats-msg-id", "def123", true)
            .send(&svc)
            .await
            .status_code
            .expect("failed to extract status code");

        let js = async_nats::jetstream::new(nats_client);

        let s = js.get_stream("foo")
            .await
            .expect("failed to get stream");

        let total_msg_count = s.get_info()
            .await
            .expect("failed to get stream info")
            .state
            .messages;

        assert_eq!(total_msg_count, 1);

        cleanup(vec!["foo".to_string()], vec![])
            .await;
    }
}
