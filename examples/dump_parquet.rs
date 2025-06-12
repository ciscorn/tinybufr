use std::{
    fs,
    io::{BufRead, BufReader},
    path::Path,
};

use arrow::record_batch::RecordBatch;
use clap::Parser;
use tinybufr::tables::local::jma::install_jma_descriptors;
use tinybufr::{DataReader, DataSpec, Error, HeaderSections, Tables, ensure_end_section};

#[derive(clap::Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Input BUFR file
    #[arg(index = 1)]
    filename: String,

    /// Output file path (.parquet or .arrow/.ipc)
    #[arg(index = 2, short, long)]
    output: Option<String>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    // Parse BUFR file into Arrow RecordBatch
    let record_batch = {
        // Extend the default tables with JMA local descriptors
        let mut tables = Tables::default();
        install_jma_descriptors(&mut tables);

        let mut reader = BufReader::new(fs::File::open(args.filename)?);

        // Check if the file starts with "BUFR", if not skip the first "local header" line (up to 1024 bytes)
        {
            let buf = reader.fill_buf()?;
            if buf.len() >= 4 && &buf[..4] != b"BUFR" {
                let max_skip = std::cmp::min(buf.len(), 1024);
                let consumed = if let Some(newline_pos) =
                    buf[..max_skip].iter().position(|&b| b == b'\n')
                {
                    newline_pos + 1
                } else if buf.len() < 1024 {
                    return Err(Error::Fatal("No BUFR data found in file".to_string()));
                } else {
                    return Err(Error::Fatal(
                        "First line too long (>1024 bytes) and doesn't start with BUFR".to_string(),
                    ));
                };
                reader.consume(consumed);
            }
        }

        let header = HeaderSections::read(&mut reader)?;
        let data_spec = DataSpec::from_data_description(&header.data_description_section, &tables)?;
        let mut data_reader = DataReader::new(&mut reader, &data_spec)?;

        let record_batch =
            tinybufr::arrow::convert_to_arrow(&mut data_reader, &tables, &data_spec)?;
        ensure_end_section(header.indicator_section.edition_number, &mut reader)?;
        record_batch
    };

    // Write output data
    if let Some(output_path) = args.output {
        write_output(&output_path, &record_batch)?;
    } else {
        // Print schema and data to stdout
        println!("Schema: {:?}", record_batch.schema());
        println!("Data: {:?}", record_batch);
    }

    Ok(())
}

fn write_output(output_path: &str, record_batch: &RecordBatch) -> Result<(), Error> {
    let path = Path::new(output_path);
    let extension = path.extension().and_then(|ext| ext.to_str()).unwrap_or("");

    match extension.to_lowercase().as_str() {
        "parquet" => {
            let file = fs::File::create(output_path)?;
            let props = parquet::file::properties::WriterProperties::builder()
                .set_compression(parquet::basic::Compression::SNAPPY)
                .build();
            let mut writer =
                parquet::arrow::ArrowWriter::try_new(file, record_batch.schema(), Some(props))
                    .map_err(|e| Error::Fatal(format!("Failed to create Parquet writer: {}", e)))?;
            writer
                .write(record_batch)
                .map_err(|e| Error::Fatal(format!("Failed to write Parquet file: {}", e)))?;
            writer
                .close()
                .map_err(|e| Error::Fatal(format!("Failed to close Parquet file: {}", e)))?;
        }
        "arrow" | "ipc" => {
            let file = fs::File::create(output_path)?;
            let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &record_batch.schema())
                .map_err(|e| Error::Fatal(format!("Failed to create Arrow writer: {}", e)))?;
            writer
                .write(record_batch)
                .map_err(|e| Error::Fatal(format!("Failed to write Arrow file: {}", e)))?;
            writer
                .finish()
                .map_err(|e| Error::Fatal(format!("Failed to finish Arrow file: {}", e)))?;
        }
        _ => {
            return Err(Error::Fatal(format!(
                "Unsupported file extension: '{}'. Use .arrow, .ipc, or .parquet",
                extension
            )));
        }
    }
    Ok(())
}
