pub mod services {
    use crate::constants::constants as consumer_constants;
    use crate::logger::logger::LoggingService;
    use crate::observable_pattern::observable_pattern::Observable;
    use rdkafka::config::ClientConfig;
    use rdkafka::error::KafkaError;
    use rdkafka::message::{BorrowedMessage, OwnedMessage, ToBytes};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use rdkafka::Message;
    use serde_json::json;
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

        fn get_payload_and_key(message: &BorrowedMessage) -> (String, String) {
            let key = std::str::from_utf8(message.key().unwrap_or(b"No Key Found").to_bytes());
            let payload =
                std::str::from_utf8(message.payload().unwrap_or(b"No Payload Found").to_bytes());
            (key.unwrap().to_string(), payload.unwrap().to_string())
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
                    let (key, payload) = ServiceMethods::get_payload_and_key(message);
                    let res = json!({
                        "message": payload,
                        "topic": topic,
                        "reply_for_key": key,
                        "status": true
                    })
                    .to_string();
                    let response = FutureRecord::to("test.reply").key(&key).payload(&res);

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
                    let (key, payload) = ServiceMethods::get_payload_and_key(message);
                    let res = json!({
                        "message": payload,
                        "topic": topic,
                        "reply_for_key": key,
                        "status": true
                    })
                    .to_string();
                    let response = FutureRecord::to("another.reply").key(&key).payload(&res);
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
