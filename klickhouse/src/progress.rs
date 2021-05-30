#[derive(Debug, Clone, Copy)]
pub struct Progress {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub new_total_rows_to_read: u64,
    pub new_written_rows: Option<u64>,
    pub new_written_bytes: Option<u64>,
}
