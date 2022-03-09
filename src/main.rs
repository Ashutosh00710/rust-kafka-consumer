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
        .set("allow.auto.create.topics", "true")
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
    // create a logger instance for main function
    let console = Logger::LoggingService {
        log_level: consumer_constants::LOG_LEVEL.to_string(),
        name: String::from("main"),
        log_for: consumer_constants::LOG_FOR.map(|f| f.to_string()).to_vec(),
    };

    // create a consumer instance
    let consumer = create_consumer();

    // create an observable instance
    let mut observable = crate::observable_pattern::observable_pattern::Observable::new();

    /// in order to pass observable as a mutable reference to multiple services
    /// we need to create a block (like from line 85 to 90) for each service
    ///
    /// reason: "cannot pass a mutable reference twice"
    {
        // create the instance of service method and pass mutable reference of observable
        // to that
        let mut service_methods =
            crate::test_service::test_service::ServiceMethods::new(&mut observable);
        service_methods.handlers();
        console.log("Initialized handlers");
    }
    // after all the blocks are done all the observers are now in place to respond to their
    // respective topics/events
    console.log("Observers are ready to receive messages");

    // to keep running this block of code use loop syntax
    loop {
        // Receives the next message from the stream
        // This method will block until the next message is available or an error occurs
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
