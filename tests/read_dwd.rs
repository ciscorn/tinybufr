use std::fs;
use std::io::{BufRead, BufReader};

use tinybufr::*;

mod common;

#[test]
fn test_synop_basic() {
    read_example(
        "./tests/testdata/dwd/synop_ISGD01_EDZW_2025_08_07_11_10.bufr",
        false,
    );
}

// This is a tiny part from "BUFR table for software package BUFR tools"
// Available at https://www.dwd.de/DE/leistungen/opendata/hilfe.html
static DWD_TABLE_B: &[TableBEntry] = &[
    TableBEntry {
        xy: XY { x: 20, y: 237 },
        class_name: "dwd",
        element_name: "METEOROLOGICAL OPTIONAL RANGE",
        unit: "M",
        scale: 0,
        reference_value: 0,
        bits: 18,
    },
    TableBEntry {
        xy: XY { x: 20, y: 238 },
        class_name: "dwd",
        element_name: "MINIMUM METEOROLOGICAL OPTIONAL RANGE",
        unit: "M",
        scale: 0,
        reference_value: 0,
        bits: 18,
    },
    TableBEntry {
        xy: XY { x: 20, y: 239 },
        class_name: "dwd",
        element_name: "MAXIMUM METEOROLOGICAL OPTIONAL RANGE",
        unit: "M",
        scale: 0,
        reference_value: 0,
        bits: 18,
    },
];

fn read_example(filename: &str, skip_first_line: bool) {
    let mut tables = Tables::default();
    for desc in DWD_TABLE_B {
        tables.table_b.insert(desc.xy, desc);
    }

    let file = fs::File::open(filename).unwrap();
    let mut reader = BufReader::new(file);
    if skip_first_line {
        // Some files have a first line that is not part of the BUFR message
        let mut buf = String::new();
        reader.read_line(&mut buf).unwrap();
    }

    common::test_full_read(reader, &tables);
}
