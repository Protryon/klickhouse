/// Query execution progress.
/// Values are delta and must be summed.
///
/// See https://clickhouse.com/codebrowser/ClickHouse/src/IO/Progress.h.html
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Progress {
    pub read_rows: u64,
    pub read_bytes: u64,
    pub new_total_rows_to_read: u64,
    pub new_written_rows: Option<u64>,
    pub new_written_bytes: Option<u64>,
}
impl std::ops::Add for Progress {
    type Output = Progress;

    fn add(self, rhs: Self) -> Self::Output {
        let sum_opt = |opt1, opt2| match (opt1, opt2) {
            (Some(a), Some(b)) => Some(a + b),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        Self::Output {
            read_rows: self.read_rows + rhs.read_rows,
            read_bytes: self.read_bytes + rhs.read_bytes,
            new_total_rows_to_read: self.new_total_rows_to_read + rhs.new_total_rows_to_read,
            new_written_rows: sum_opt(self.new_written_rows, rhs.new_written_rows),
            new_written_bytes: sum_opt(self.new_written_bytes, rhs.new_written_bytes),
        }
    }
}

impl std::ops::AddAssign for Progress {
    fn add_assign(&mut self, rhs: Self) {
        *self = *self + rhs;
    }
}
