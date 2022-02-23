pub mod services {
    use crate::constants::constants as consumer_constants;
    use crate::logger::logger::LoggingService;
    use crate::observable_pattern::observable_pattern::Observable;
    use rdkafka::config::ClientConfig;
    use rdkafka::error::KafkaError;
    use rdkafka::message::{BorrowedMessage, OwnedMessage};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::Message;
    use std::time::Duration;

    pub struct ServiceMethods<'a> {
        observable: &'a mut Observable,
        producer: FutureProducer,
    }

    impl ServiceMethods<'_> {
        pub fn new(observable: &mut Observable) -> ServiceMethods {
            ServiceMethods {
                observable,
                producer: ServiceMethods::create_producer(),
            }
        }

        fn create_producer() -> FutureProducer {
            ClientConfig::new()
                .set("bootstrap.servers", consumer_constants::BROKERS)
                .set("queue.buffering.max.ms", "0") // Do not buffer
                .create()
                .expect("Producer creation failed")
        }

        fn subscribe(
            &mut self,
            callback: Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        ) {
            self.observable.subscribe(callback);
        }

        pub fn handlers(&mut self) {
            let producer = self.producer.clone();
            self.subscribe(Box::new(move |topic, message| {
                if topic == consumer_constants::topic::TEST {
                    let console = LoggingService {
                        log_level: String::from("DEV"),
                        name: String::from("(service) topic: test"),
                        log_for: vec!["DEV".to_string(), "STAGE".to_string()],
                    };
                    console.log(format!("Listened by topic: {}", topic));
                    let mut response = FutureRecord::to("test.reply");
                    if let Some(p) = message.payload() {
                        response = response.payload(p);
                    }
                    if let Some(k) = message.key() {
                        response = response.key(k);
                    }

                    let reply_topic = response.topic;
                    let result = futures::executor::block_on(
                        producer.send(response, Duration::from_secs(1)),
                    );
                    match result {
                        Ok(res) => {
                            console.log(format!("Response sent to: {}", reply_topic));
                            Some(Ok(res))
                        }
                        Err(e) => {
                            console.log(format!("Error sending response to -> {}", reply_topic));
                            Some(Err(e))
                        }
                    }
                } else {
                    None
                }
            }));

            let producer2 = self.producer.clone();
            self.subscribe(Box::new(move |topic, message| {
                if topic == consumer_constants::topic::ANOTHER {
                    let console = LoggingService {
                        log_level: String::from("DEV"),
                        name: String::from("(service) topic: another"),
                        log_for: vec!["DEV".to_string(), "STAGE".to_string()],
                    };
                    console.log(format!("Listened by topic: {}", topic));
                    let mut response = FutureRecord::to("another.reply");
                    if let Some(p) = message.payload() {
                        response = response.payload(p);
                    }
                    if let Some(k) = message.key() {
                        response = response.key(k);
                    }

                    let reply_topic = response.topic;
                    let result = futures::executor::block_on(
                        producer2.send(response, Duration::from_secs(1)),
                    );

                    match result {
                        Ok(res) => {
                            console.log(format!("Response sent to: {}", reply_topic));
                            Some(Ok(res))
                        }
                        Err(e) => {
                            console.log(format!("Error sending response to -> {}", reply_topic));
                            Some(Err(e))
                        }
                    }
                } else {
                    None
                }
            }));
        }
    }
}
