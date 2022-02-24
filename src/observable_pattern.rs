pub mod observable_pattern {
    use rdkafka::{
        error::KafkaError,
        message::{BorrowedMessage, OwnedMessage}, producer::FutureProducer, ClientConfig,
    };
    use crate::constants::constants as consumer_constants;
    use crate::logger::logger::LoggingService;

    pub struct Observable {
        callbacks: Vec<
            Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                    FutureProducer
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        >,
        producer: FutureProducer,
    }

    impl Observable {
        pub fn new() -> Observable {
            Observable {
                callbacks: Vec::new(),
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
            callback: Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                    FutureProducer,
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        ) {
            self.callbacks.push(callback);
        }

        pub fn emit(&self, message: &BorrowedMessage, topic: &str) {
            let console = LoggingService {
                log_level: String::from("DEV"),
                name: String::from("(observable) emit"),
                log_for: vec!["DEV".to_string(), "STAGE".to_string()],
            };
            console.log("Delivering to subscribers");
            for callback in &self.callbacks {
                callback(topic, message, self.producer.clone());
            }
        }
    }
}
