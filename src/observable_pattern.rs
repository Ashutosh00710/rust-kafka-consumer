pub mod observable_pattern {
    use rdkafka::{
        error::KafkaError,
        message::{BorrowedMessage, OwnedMessage},
    };

    use crate::logger::logger::LoggingService;

    pub struct Observable {
        callbacks: Vec<
            Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
                ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>>,
            >,
        >,
    }

    impl Observable {
        pub fn new() -> Observable {
            Observable {
                callbacks: Vec::new(),
            }
        }

        pub fn subscribe(
            &mut self,
            callback: Box<
                dyn Fn(
                    &str,
                    &BorrowedMessage,
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
                callback(topic, message);
            }
        }
    }
}
