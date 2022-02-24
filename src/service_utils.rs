pub mod service_utils {
    use rdkafka::{
        error::KafkaError,
        message::{BorrowedMessage, OwnedMessage, ToBytes},
        Message,
    };

    use crate::logger::logger::LoggingService;

    pub fn get_payload_and_key(message: &BorrowedMessage) -> (String, String) {
        let key = std::str::from_utf8(message.key().unwrap_or(b"No Key Found").to_bytes());
        let payload =
            std::str::from_utf8(message.payload().unwrap_or(b"No Payload Found").to_bytes());
        (key.unwrap().to_string(), payload.unwrap().to_string())
    }

    pub fn return_result(
        result: Result<(i32, i64), (KafkaError, OwnedMessage)>,
        console: LoggingService,
        reply_topic: &str,
    ) -> Option<Result<(i32, i64), (KafkaError, OwnedMessage)>> {
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
    }
}
