pub mod observable_pattern {
    use std::collections::HashMap;

    use crate::constants::constants as consumer_constants;
    use crate::logger::logger::LoggingService;
    use rdkafka::{
        error::KafkaError,
        message::{BorrowedMessage, OwnedMessage},
        producer::FutureProducer,
        ClientConfig,
    };

    pub struct Observable {
        callbacks: HashMap<
            String,
            Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                    FutureProducer,
                    LoggingService,
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        >,
        producer: FutureProducer,
    }

    impl Observable {
        pub fn new() -> Observable {
            Observable {
                callbacks: HashMap::new(),
                producer: Observable::create_producer(),
            }
        }

        fn create_producer() -> FutureProducer {
            ClientConfig::new()
                .set("bootstrap.servers", consumer_constants::BROKERS)
                .set("queue.buffering.max.ms", "0") // Do not buffer
                .create()
                .expect("Producer creation failed")
        }

        pub fn subscribe(
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
            self.callbacks.insert(topic.to_string(), callback);
        }

        pub fn emit(&self, message: &BorrowedMessage, topic: &str) {
            let console = LoggingService {
                log_level: consumer_constants::LOG_LEVEL.to_string(),
                name: String::from("(observable) emit"),
                log_for: consumer_constants::LOG_FOR.map(|f| f.to_string()).to_vec(),
            };
            console.log("Delivering to subscriber for topic: ".to_string() + topic);
            let callback = self.callbacks.get(topic);
            if let Some(callback) = callback {
                let console = LoggingService {
                    log_level: consumer_constants::LOG_LEVEL.to_string(),
                    name: String::from(format!("(service) topic: {}", topic)),
                    log_for: consumer_constants::LOG_FOR.map(|f| f.to_string()).to_vec(),
                };
                let result = callback(topic, message, self.producer.clone(), console.clone());
                if let Some(result) = result {
                    if let Err(e) = result {
                        console.error(format!("Error: {}", e.0));
                    }
                }
            }
        }
    }
}
