use std::fs;
use std::io::{BufRead, BufReader};

use tinybufr::tables::local::jma::{JMA_DATA_DESCRIPTORS, JMA_SEQUENCE_DESCRIPTORS};
use tinybufr::*;

mod common;

#[test]
fn test_amedas() {
    read_example(
        "./tests/testdata/jma/Z__C_RJTD_20210918110000_OBS_AMDS_Rjp_N2_bufr4.bin",
        false,
    );
    read_example(
        "./tests/testdata/jma/Z__C_RJTD_20210918110000_OBS_AMDSRR_Rjp_N1_bufr4.bin",
        false,
    );
}

#[test]
fn test_wind_profiler() {
    read_example(
        "./tests/testdata/jma/Z__C_RJTD_20200728040000_WPR_SEQ_RS-all_Pww_bufr4.bin",
        false,
    );
}

#[test]
fn test_ryuikishisu() {
    read_example(
        "./tests/testdata/jma/Z__C_RJTD_20230815070000_MET_SEQ_Ggis1km_Proi_Aper10min_RJsuikei830_ANAL_bufr4.bin",
        false,
    );
    read_example(
        "./tests/testdata/jma/Z__C_RJTD_20230815070000_MET_SEQ_Ggis1km_Proi_Fper10min_RJsuikei811_FH0010-0100_bufr4.bin",
        false,
    );
}

#[test]
fn test_istc62() {
    read_example(
        "./tests/testdata/jma/ISTC62_RJTD_310000_201707310002140_001_93839.bin",
        true,
    );
}

#[test]
fn test_istc82() {
    read_example("./tests/testdata/jma/ISTC82.dat", true);
}

#[test]
fn test_ixac41() {
    read_example(
        "./tests/testdata/jma/201806180758.20230110141530_520.BUFR",
        false,
    );
}

fn read_example(filename: &str, skip_first_line: bool) {
    // Extend the default tables with JMA local descriptors
    let mut tables = Tables::default();
    for desc in &JMA_DATA_DESCRIPTORS {
        tables.table_b.insert(desc.xy, desc);
    }
    for seq in &JMA_SEQUENCE_DESCRIPTORS {
        tables.table_d.insert(seq.xy, seq);
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
