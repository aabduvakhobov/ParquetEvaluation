use std::ops::Range;

use parquet::{basic::Type, data_type::ByteArray, schema::types::SchemaDescPtr};

/// Values are stored densely (nulls only appear in the definition levels), so the number of values
/// in a row range is the number of rows whose definition level is the maximum.
pub fn count_values(def_levels: &[i16], max_def_level: i16, rows: Range<usize>) -> usize {
    if max_def_level == 0 {
        return rows.len();
    }
    def_levels[rows].iter().filter(|&&d| d == max_def_level).count()
}

/// Levels are empty when the column is required (definition) or flat (repetition).
pub fn level_slice(levels: &[i16], rows: Range<usize>) -> &[i16] {
    if levels.is_empty() { levels } else { &levels[rows] }
}

pub enum ColumnBuffer {
    TimeStampBuffer {
        values: Vec<i64>,
        def_levels: Vec<i16>,
        rep_levels: Vec<i16>,
    },
    ValueBuffer {
        values: Vec<f32>,
        def_levels: Vec<i16>,
        rep_levels: Vec<i16>,
    },
    StringBuffer {
        values: Vec<ByteArray>,
        def_levels: Vec<i16>,
        rep_levels: Vec<i16>,
    },
}

impl ColumnBuffer {
    pub fn read_records(&self, num_records: usize) {
        match self {
            ColumnBuffer::TimeStampBuffer { values, .. } => {
                println!("First 10 records: {:?}", values.get(0..10.min(num_records)));
            }
            ColumnBuffer::ValueBuffer { values, .. } => {
                println!("First 10 records: {:?}", values.get(0..10.min(num_records)));
            }
            ColumnBuffer::StringBuffer { values, .. } => {
                println!("First 10 records: {:?}", values.get(0..10.min(num_records)));
            }
        }
    }

    pub fn clear(&mut self) {
        match self {
            ColumnBuffer::TimeStampBuffer { values, def_levels, rep_levels, .. } => {
                values.clear();
                def_levels.clear();
                rep_levels.clear();
            }
            ColumnBuffer::ValueBuffer { values, def_levels, rep_levels, .. } => {
                values.clear();
                def_levels.clear();
                rep_levels.clear();            
            }
            ColumnBuffer::StringBuffer { values, def_levels, rep_levels, .. } => {
                values.clear();
                def_levels.clear();
                rep_levels.clear();            
            }
        }
    }

    /// Remove the first `n` rows; with nulls this is fewer than `n` values.
    pub fn drain(&mut self, n: usize, max_def_level: i16) {
        fn drain_rows<T>(values: &mut Vec<T>, def_levels: &mut Vec<i16>, rep_levels: &mut Vec<i16>, n: usize, max_def_level: i16) {
            values.drain(..count_values(def_levels, max_def_level, 0..n));
            if !def_levels.is_empty() {
                def_levels.drain(..n);
            }
            if !rep_levels.is_empty() {
                rep_levels.drain(..n);
            }
        }
        match self {
            ColumnBuffer::TimeStampBuffer { values, def_levels, rep_levels } => drain_rows(values, def_levels, rep_levels, n, max_def_level),
            ColumnBuffer::ValueBuffer { values, def_levels, rep_levels } => drain_rows(values, def_levels, rep_levels, n, max_def_level),
            ColumnBuffer::StringBuffer { values, def_levels, rep_levels } => drain_rows(values, def_levels, rep_levels, n, max_def_level),
        }
    }
}
pub struct RowGroupBuffer {
    pub buffer_len: i64,
    pub columns: Vec<ColumnBuffer>,
    pub max_def_levels: Vec<i16>,
}

impl RowGroupBuffer {
    pub fn new(num_columns: usize, schema_descr: &SchemaDescPtr) -> Self {
        let mut columns = Vec::with_capacity(num_columns);
        let max_def_levels = schema_descr.columns().iter().map(|col| col.max_def_level()).collect();
        for col in schema_descr.columns() {
            match col.physical_type() {
                Type::INT64 => columns.push(ColumnBuffer::TimeStampBuffer {
                    values: Vec::new(),
                    def_levels: Vec::new(),
                    rep_levels: Vec::new(),
                }),
                Type::FLOAT => columns.push(ColumnBuffer::ValueBuffer {
                    values: Vec::new(),
                    def_levels: Vec::new(),
                    rep_levels: Vec::new(),
                }),
                Type::BYTE_ARRAY => columns.push(ColumnBuffer::StringBuffer {
                    values: Vec::new(),
                    def_levels: Vec::new(),
                    rep_levels: Vec::new(),
                }),
                _ => unimplemented!("Unsupported column type"),
            }
        }

        Self {
            buffer_len: 0,
            columns,
            max_def_levels,
        }
    }

    pub fn clear(&mut self) {
        for column in &mut self.columns {
            column.clear();
        }
        self.buffer_len = 0;
    }

    pub fn clear_until_n(&mut self, n: usize) {
        for (column, &max_def_level) in self.columns.iter_mut().zip(&self.max_def_levels) {
            column.drain(n, max_def_level);
        }
        self.buffer_len -= i64::try_from(n).unwrap();
    }
}
