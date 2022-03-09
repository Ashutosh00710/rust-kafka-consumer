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

    /// AIM: To implement the observer pattern for the Kafka Consumer
    /// Idea:
    /// Against every topic we have a callback function which is called when a message is received
    /// HashMap<Topic, Function>
    /// {
    ///     [topic: of type String]: [function: a closure that will process the message and produce a response to Kafka reply topic]
    /// }
    /// For Example:
    /// {
    ///     "some_topic": |topic, message, producer, console| { /* some processing */ }
    /// }
    ///
    /// After the subscription of all the topics, this HashMap is ready to deliver the
    /// messages to the respective callback function in O(1) time.
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

        // create the producer for the callback functions to send the response to the
        // Kafka reply topic
        fn create_producer() -> FutureProducer {
            ClientConfig::new()
                .set("bootstrap.servers", consumer_constants::BROKERS)
                .set("queue.buffering.max.ms", "0") // Do not buffer
                .create()
                .expect("Producer creation failed")
        }

        // to push the callback function to the HashMap against the topic
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

        // deliver the message to the respective callback function
        pub fn emit(&self, message: &BorrowedMessage, topic: &str) {
            // create logging instance
            let console = LoggingService {
                log_level: consumer_constants::LOG_LEVEL.to_string(),
                name: String::from("(observable) emit"),
                log_for: consumer_constants::LOG_FOR.map(|f| f.to_string()).to_vec(),
            };
            console.log("Delivering to subscriber for topic: ".to_string() + topic);
            // get the callback function for the topic
            let callback = self.callbacks.get(topic);
            // check for the callback function
            if let Some(callback) = callback {
                // create the logging instance for the callback function
                let console = LoggingService {
                    log_level: consumer_constants::LOG_LEVEL.to_string(),
                    name: String::from(format!("(service) topic: {}", topic)),
                    log_for: consumer_constants::LOG_FOR.map(|f| f.to_string()).to_vec(),
                };
                // call the callback function and wait for the response to be sent to Kafka reply topic
                let result = callback(topic, message, self.producer.clone(), console.clone());
                // check for the result
                if let Some(result) = result {
                    // if the result is Err, log the error
                    if let Err(e) = result {
                        console.error(format!("Error: {}", e.0));
                    }
                }
            } else {
                // if callback is not found, log the error
                console.error(format!("Callback not found for topic: {}", topic));
            }
        }
    }
}
