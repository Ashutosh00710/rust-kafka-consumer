pub mod observable_pattern;
pub mod services;
use crate::logger::logger as Logger;
pub mod logger;

use log::{info, warn};

use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::stream_consumer::StreamConsumer;
use rdkafka::consumer::{Consumer, ConsumerContext};
use rdkafka::error::KafkaResult;
use rdkafka::topic_partition_list::TopicPartitionList;
use rdkafka::Message;

struct LoggingConsumerContext;

impl ClientContext for LoggingConsumerContext {}

impl ConsumerContext for LoggingConsumerContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => info!("Offsets committed successfully"),
            Err(e) => warn!("Error while committing offsets: {}", e),
        };
    }
}

// Define a new type for convenience
type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

fn create_consumer(brokers: &str, group_id: &str, topic: Vec<&str>) -> LoggingConsumer {
    let context = LoggingConsumerContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", group_id)
        .set("bootstrap.servers", brokers)
        .set("enable.partition.eof", "false")
        .set("session.timeout.ms", "6000")
        // Commit automatically every 5 seconds.
        .set("enable.auto.commit", "true")
        .set("auto.commit.interval.ms", "5000")
        // but only commit the offsets explicitly stored via `consumer.store_offset`.
        .set("enable.auto.offset.store", "false")
        .set_log_level(RDKafkaLogLevel::Debug)
        .create_with_context(context)
        .expect("Consumer creation failed");

    consumer
        .subscribe(&topic)
        .expect("Can't subscribe to specified topic");

    consumer
}

#[tokio::main]
async fn main() {
    const BROKERS: &str = "localhost:9092,localhost:9093,localhost:9094";
    const GROUP_ID: &str = "rust-consumer-group";
    let subscribe_to_topics = vec!["test", "another"];
    let console = Logger::LoggingService {
        log_level: String::from("DEV"),
        name: String::from("main"),
        log_for: vec!["DEV".to_string(), "STAGE".to_string()],
    };

    let consumer = create_consumer(BROKERS, GROUP_ID, subscribe_to_topics);

    let mut observable = crate::observable_pattern::observable_pattern::Observable::new();
    {
        let mut service_methods = crate::services::services::ServiceMethods::new(&mut observable);
        service_methods.handlers();
        console.log("Initialized handlers");
    }
    console.log("Observers are ready to receive messages");

    loop {
        match consumer.recv().await {
            Err(e) => {
                console.error(format!("Kafka error: {}", e));
            }
            Ok(m) => {
                console.log(format!("Message received from: {}", m.topic()));
                observable.emit(&m, m.topic());
            }
        }
    }
}
