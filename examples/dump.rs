use std::fs;
use std::io::{BufRead, BufReader};

use clap::Parser;
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

fn main() -> Result<(), Error> {
    let args = Args::parse();

    #[allow(unused_mut)]
    let mut tables = Tables::default();
    #[cfg(feature = "jma")]
    tinybufr::tables::local::jma::install_jma_descriptors(&mut tables);

    let file = fs::File::open(args.filename).unwrap();
    let mut reader = BufReader::new(file);

    // Check if the file starts with "BUFR", if not skip the first line (up to 1024 bytes)
    {
        let buf = reader.fill_buf()?;
        if buf.len() >= 4 && &buf[..4] != b"BUFR" {
            // File doesn't start with BUFR, skip to the next line
            let max_skip = buf.len().min(1024);
            let consumed =
                if let Some(newline_pos) = buf[..max_skip].iter().position(|&b| b == b'\n') {
                    // Found newline within limit, skip past it
                    newline_pos + 1
                } else if buf.len() < 1024 {
                    // Reached EOF without finding newline
                    return Err(Error::Fatal("No BUFR data found in file".to_string()));
                } else {
                    // No newline found within 1024 bytes
                    return Err(Error::Fatal(
                        "First line too long (>1024 bytes) and doesn't start with BUFR".to_string(),
                    ));
                };

            reader.consume(consumed);
        }
    }

    // Parse header sections
    let header = HeaderSections::read(&mut reader).unwrap();
    println!("{header:#?}");

    // Parse data section
    let data_spec =
        DataSpec::from_data_description(&header.data_description_section, &tables).unwrap();
    let mut data_reader = DataReader::new(&mut reader, &data_spec).unwrap();

    loop {
        match data_reader.read_event() {
            Ok(DataEvent::Data { idx: _, xy, value }) => {
                if let Some(b) = tables.table_b.get(&xy) {
                    println!("Data {} = {:?} [{}]", b.element_name, value, b.unit);
                } else {
                    println!("Data: {value:?}");
                };
            }
            Ok(DataEvent::CompressedData { idx: _, xy, values }) => {
                if let Some(b) = tables.table_b.get(&xy) {
                    println!("Data {} = {:?} {}", b.element_name, values, b.unit);
                } else {
                    println!("Data: {values:?}");
                };
            }
            Ok(DataEvent::Eof) => {
                break;
            }
            Ok(_) => {}
            Err(e) => return Err(e),
        }
    }

    if let Err(err) = ensure_end_section(header.indicator_section.edition_number, &mut reader) {
        eprintln!("Error: {err:?}");
    }
    Ok(())
}
