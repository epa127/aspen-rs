pub mod client;
pub mod server;
pub mod packet;
pub mod store;

const CAPACITY: usize = 8500000;
const BE_BYTE: u8 = 6;
const LC_READ_BYTE: u8 = 7;
const LC_WRITE_BYTE: u8 = 8;
const NONE_BYTE: u8 = 0;
const SOME_BYTE: u8 = 1;
const SUBSTRING_LEN: usize = 3;
const BUF_LEN: usize = 512;
const LEN_LENGTH: usize = size_of::<u64>();
const SIG_FIG: u8 = 3;
const YIELD_FREQ: usize = 5; // yield every 2^n best effort sub-operations