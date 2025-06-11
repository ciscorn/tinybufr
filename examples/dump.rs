use std::fs;
use std::io::{BufRead, BufReader};

use clap::Parser;
use tinybufr::tables::local::jma::{JMA_DATA_DESCRIPTORS, JMA_SEQUENCE_DESCRIPTORS};
use tinybufr::*;

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input BUFR file path
    #[arg(index = 1)]
    filename: String,

    /// Skip first line of input
    #[arg(short, long)]
    skip_first_line: bool,
}

fn main() {
    let args = Args::parse();

    // Extend the default tables with JMA local descriptors
    let mut tables = Tables::default();
    for desc in &JMA_DATA_DESCRIPTORS {
        tables.table_b.insert(desc.xy, desc);
    }
    for seq in &JMA_SEQUENCE_DESCRIPTORS {
        tables.table_d.insert(seq.xy, seq);
    }

    let file = fs::File::open(args.filename).unwrap();
    let mut reader = BufReader::new(file);
    if args.skip_first_line {
        // Some files have a first line that is not part of the BUFR message
        let mut buf = String::new();
        reader.read_line(&mut buf).unwrap();
    }

    // Parse header sections
    let header = HeaderSections::read(&mut reader).unwrap();
    println!("{:#?}", header);

    // Parse data section
    let data_spec =
        DataSpec::from_data_description(&header.data_description_section, &tables).unwrap();
    let mut data_reader = DataReader::new(&mut reader, &data_spec).unwrap();

    loop {
        match data_reader.read_event() {
            Ok(DataEvent::Data { idx: _, xy, value }) => {
                if let Some(b) = tables.table_b.get(&xy) {
                    println!("Data: {} {:?} {}", b.element_name, value, b.unit);
                } else {
                    println!("Data: {:?}", value);
                };
            }
            Ok(DataEvent::CompressedData { idx: _, xy, values }) => {
                if let Some(b) = tables.table_b.get(&xy) {
                    println!("Data: {} {:?} {}", b.element_name, values, b.unit);
                } else {
                    println!("Data: {:?}", values);
                };
            }
            Ok(DataEvent::Eof) => {
                break;
            }
            Ok(_) => {}
            Err(e) => {
                eprintln!("Error: {:?}", e);
                return;
            }
        }
    }

    if let Err(err) = ensure_end_section(header.indicator_section.edition_number, &mut reader) {
        eprintln!("Error: {:?}", err);
    }
}
