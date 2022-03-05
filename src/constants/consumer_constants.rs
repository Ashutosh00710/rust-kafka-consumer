pub mod constants {
    pub const BROKERS: &str = "localhost:9092,localhost:9093,localhost:9094";
    pub const GROUP_ID: &str = "rust-consumer-group";
    pub const SUBSCRIBE_TO_TOPICS: [&str; 2] = ["test", "another"];
    pub const LOG_LEVEL: &str = "DEV";
    pub const LOG_FOR: [&str; 2] = ["DEV", "STAGE"];
    pub mod topic {
        pub const TEST: &str = "test";
        pub const ANOTHER: &str = "another";
    }
}
