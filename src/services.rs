pub mod services {
    use crate::constants::constants as consumer_constants;
    use crate::logger::logger::LoggingService;
    use crate::observable_pattern::observable_pattern::Observable;
    use crate::service_utils::service_utils::{get_payload_and_key, return_result};
    use rdkafka::error::KafkaError;
    use rdkafka::message::{BorrowedMessage, OwnedMessage};
    use rdkafka::producer::{FutureProducer, FutureRecord};
    use serde_json::json;
    use std::time::Duration;

    pub struct ServiceMethods<'a> {
        observable: &'a mut Observable,
    }

    impl ServiceMethods<'_> {
        pub fn new(observable: &mut Observable) -> ServiceMethods {
            ServiceMethods { observable }
        }

        fn subscribe(
            &mut self,
            callback: Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                    FutureProducer,
                    LoggingService,
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        ) {
            self.observable.subscribe(callback);
        }

        pub fn handlers(&mut self) {
            self.subscribe(Box::new(move |topic, message, producer, console| {
                if topic == consumer_constants::topic::TEST {
                    console.log(format!("Listened by topic: {}", topic));
                    let (key, payload) = get_payload_and_key(message);
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

                    return_result(result, console, reply_topic)
                } else {
                    None
                }
            }));

            self.subscribe(Box::new(move |topic, message, producer, console| {
                if topic == consumer_constants::topic::ANOTHER {
                    console.log(format!("Listened by topic: {}", topic));
                    let (key, payload) = get_payload_and_key(message);
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
                        producer.send(response, Duration::from_secs(1)),
                    );

                    return_result(result, console, reply_topic)
                } else {
                    None
                }
            }));
        }
    }
}
