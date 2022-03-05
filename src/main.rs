#[path = "./constants/consumer_constants.rs"]
pub mod constants;
#[path = "./logger/logger.rs"]
pub mod logger;
#[path = "./observable/observable_pattern.rs"]
pub mod observable_pattern;
#[path = "./utils/service_utils.rs"]
pub mod service_utils;
#[path = "./services/test_service.rs"]
pub mod test_service;

use crate::constants::constants as consumer_constants;
use crate::logger::logger as Logger;
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
        let console = Logger::LoggingService {
            log_level: String::from("DEV"),
            name: String::from("main"),
            log_for: vec!["DEV".to_string(), "STAGE".to_string()],
        };
        match result {
            Ok(_) => console.log("Offsets committed successfully"),
            Err(e) => console.error(format!("Error while committing offsets: {}", e)),
        };
    }
}

// Define a new type for convenience
type LoggingConsumer = StreamConsumer<LoggingConsumerContext>;

fn create_consumer() -> LoggingConsumer {
    let context = LoggingConsumerContext;

    let consumer: LoggingConsumer = ClientConfig::new()
        .set("group.id", consumer_constants::GROUP_ID)
        .set("bootstrap.servers", consumer_constants::BROKERS)
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
        .subscribe(&consumer_constants::SUBSCRIBE_TO_TOPICS)
        .expect("Can't subscribe to specified topic");

    consumer
}

#[tokio::main]
async fn main() {
    let console = Logger::LoggingService {
        log_level: String::from("DEV"),
        name: String::from("main"),
        log_for: vec!["DEV".to_string(), "STAGE".to_string()],
    };

    let consumer = create_consumer();

    let mut observable = crate::observable_pattern::observable_pattern::Observable::new();
    {
        let mut service_methods =
            crate::test_service::test_service::ServiceMethods::new(&mut observable);
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
