pub mod client;
pub mod server;
pub mod packet;

const CAPACITY: usize = 8500000;
const BE_BYTE: u8 = 6;
const LC_BYTE: u8 = 7;
const SUBSTRING_LEN: usize = 3;
const BUF_LEN: usize = 512;
const LEN_LENGTH: usize = size_of::<u64>();
const SIG_FIG: u8 = 3;