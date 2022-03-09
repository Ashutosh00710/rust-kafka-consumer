pub mod test_service {
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
        // initialize the service with the observable as a mutable reference
        pub fn new(observable: &mut Observable) -> ServiceMethods {
            ServiceMethods { observable }
        }

        fn subscribe(
            &mut self,
            topic: &str,
            callback: Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                    FutureProducer,
                    LoggingService,
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        ) {
            self.observable.subscribe(topic, callback);
        }

        // write all the handlers for the service
        pub fn handlers(&mut self) {
            self.subscribe(
                consumer_constants::topic::TEST,
                Box::new(move |topic, message, producer, console| {
                    console.log(format!("Listened by topic: {}", topic));

                    // get the payload and key from the message
                    let (key, payload) = get_payload_and_key(message);

                    // this is just a test but we can do any kind of processing here and
                    // return the result
                    let res = json!({
                        "message": payload,
                        "topic": topic,
                        "reply_for_key": key,
                        "status": true
                    })
                    .to_string();

                    // prepare reply to produce the result to the corresponding reply topic
                    let response = FutureRecord::to("test.reply").key(&key).payload(&res);

                    let reply_topic = response.topic;
                    // produce the result
                    // TODO: if possible use async/await instead of futures::executor::block_on method
                    // NOTE: this is blocking and we are using block_on method to wait for the
                    // future to complete (maybe we can use async/await but I haven't figured it
                    // out yet)
                    let result = futures::executor::block_on(
                        producer.send(response, Duration::from_secs(1)),
                    );

                    // return the result of produced message
                    return_result(result, console, reply_topic)
                }),
            );

            self.subscribe(
                consumer_constants::topic::ANOTHER,
                Box::new(move |topic, message, producer, console| {
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
                }),
            );
        }
    }
}
