use std::fs;
use std::io::{BufRead, BufReader, Read};

use clap::Parser;
use serde::Serialize;
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

    /// Decode only the handler sections
    #[arg(short, long)]
    only_header: bool,
}

type Subsets = Vec<Sequence>;

type Sequence = indexmap::IndexMap<String, Value>;

#[derive(Debug, Serialize)]
#[serde(untagged)]
enum Value {
    Missing(()),
    Float(f64),
    Integer(i32),
    String(String),
    Replication(Vec<Sequence>),
    Sequence(Sequence),
    CompressedData(Vec<Value>),
}

#[derive(Debug, Serialize)]
struct JsonBody {
    header: HeaderSections,
    subsets: Option<Subsets>,
    compressed: Option<Sequence>,
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    // Extend the default tables with JMA local descriptors
    let mut tables = Tables::default();
    for desc in &JMA_DATA_DESCRIPTORS {
        tables.table_b.insert(desc.xy, desc);
    }
    for seq in &JMA_SEQUENCE_DESCRIPTORS {
        tables.table_d.insert(seq.xy, seq);
    }

    let file = fs::File::open(args.filename)?;
    let mut reader = BufReader::new(file);
    if args.skip_first_line {
        // Some files have a first line that is not part of the BUFR message
        let mut buf = String::new();
        reader.read_line(&mut buf)?;
    }

    // Parse header sections
    let header = HeaderSections::read(&mut reader)?;

    if args.only_header {
        let Ok(json) = serde_json::to_string_pretty(&header) else {
            return Err(Error::Fatal("Failed to serialize to JSON".to_string()));
        };
        println!("{}", json);
        return Ok(());
    }

    // Parse data section
    let data_spec = DataSpec::from_data_description(&header.data_description_section, &tables)?;

    let mut data_reader = DataReader::new(&mut reader, &data_spec)?;
    let mut subsets = Subsets::new();
    let mut compressed: Option<Sequence> = None;

    loop {
        match data_reader.read_event()? {
            DataEvent::SubsetStart(_) => {
                let subset = parse_sequence(&mut data_reader, &tables)?;
                subsets.push(subset);
            }
            DataEvent::CompressedStart => {
                compressed = Some(parse_sequence(&mut data_reader, &tables)?);
            }
            DataEvent::Eof => {
                break;
            }
            ev => {
                unreachable!("Unexpected data event: {:#?}", ev);
            }
        }
    }

    drop(data_reader);
    ensure_end_section(header.indicator_section.edition_number, &mut reader)?;

    let body = JsonBody {
        header,
        subsets: if subsets.is_empty() {
            None
        } else {
            Some(subsets)
        },
        compressed,
    };
    let Ok(json) = serde_json::to_string_pretty(&body) else {
        return Err(Error::Fatal("Failed to serialize to JSON".to_string()));
    };
    println!("{}", json);

    Ok(())
}

fn parse_sequence<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
) -> Result<Sequence, Error> {
    let mut subset = Sequence::new();
    let mut element_name_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut sequence_title_counts: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();
    let mut replication_count: usize = 0;

    loop {
        match data_reader.read_event()? {
            DataEvent::SubsetEnd | DataEvent::SequenceEnd | DataEvent::ReplicationItemEnd => break,
            DataEvent::Data { value, xy, .. } => {
                let Some(b) = tables.table_b.get(&xy) else {
                    return Err(Error::Fatal(format!("Unknown data descriptor: {:#?}", xy)));
                };

                // Track element name occurrences
                let count = element_name_counts
                    .entry(b.element_name.to_string())
                    .or_insert(0);
                *count += 1;

                // Create the label with unit between name and counter
                let label = match b.unit {
                    "Numeric" => {
                        if *count > 1 {
                            format!("{} ({})", b.element_name, count)
                        } else {
                            b.element_name.to_string()
                        }
                    }
                    _ => {
                        if *count > 1 {
                            format!("{} [{}] ({})", b.element_name, b.unit, count)
                        } else {
                            format!("{} [{}]", b.element_name, b.unit)
                        }
                    }
                };
                let value = match value {
                    tinybufr::Value::Missing => Value::Missing(()),
                    tinybufr::Value::Decimal(v, s) => {
                        if s >= 0 {
                            Value::Integer(v * 10f64.powi(s as i32) as i32)
                        } else {
                            Value::Float(v as f64 * 10f64.powi(s as i32))
                        }
                    }
                    tinybufr::Value::Integer(v) => Value::Integer(v),
                    tinybufr::Value::String(v) => Value::String(v.clone()),
                };
                subset.insert(label, value);
            }
            DataEvent::CompressedData { xy, values, .. } => {
                let Some(b) = tables.table_b.get(&xy) else {
                    return Err(Error::Fatal(format!("Unknown data descriptor: {:#?}", xy)));
                };

                // Track element name occurrences
                let count = element_name_counts
                    .entry(b.element_name.to_string())
                    .or_insert(0);
                *count += 1;

                // Create the label with unit between name and counter
                let label = match b.unit {
                    "Numeric" => {
                        if *count > 1 {
                            format!("{} ({})", b.element_name, count)
                        } else {
                            b.element_name.to_string()
                        }
                    }
                    _ => {
                        if *count > 1 {
                            format!("{} [{}] ({})", b.element_name, b.unit, count)
                        } else {
                            format!("{} [{}]", b.element_name, b.unit)
                        }
                    }
                };
                let vals: Vec<Value> = values
                    .into_iter()
                    .map(|v| match v {
                        tinybufr::Value::Missing => Value::Missing(()),
                        tinybufr::Value::Decimal(v, s) => {
                            if s >= 0 {
                                Value::Integer(v * 10f64.powi(s as i32) as i32)
                            } else {
                                Value::Float(v as f64 * 10f64.powi(s as i32))
                            }
                        }
                        tinybufr::Value::Integer(v) => Value::Integer(v),
                        tinybufr::Value::String(v) => Value::String(v.clone()),
                    })
                    .collect();
                subset.insert(label, Value::CompressedData(vals));
            }
            DataEvent::SequenceStart { xy, .. } => {
                let Some(d) = tables.table_d.get(&xy) else {
                    return Err(Error::Fatal(format!(
                        "Unknown sequence descriptor: {:#?}",
                        xy
                    )));
                };

                // Track sequence title occurrences
                let count = sequence_title_counts
                    .entry(d.title.to_string())
                    .or_insert(0);
                *count += 1;

                // Create the label with counter if duplicate
                let label = if *count > 1 {
                    format!("{} ({})", d.title, count)
                } else {
                    d.title.to_string()
                };

                let sequence = parse_sequence(data_reader, tables)?;
                subset.insert(label, Value::Sequence(sequence));
            }
            DataEvent::ReplicationStart { .. } => {
                replication_count += 1;
                let label = format!("replication:{}", replication_count);
                let replication = parse_replication(data_reader, tables)?;
                subset.insert(label, Value::Replication(replication));
            }
            DataEvent::OperatorHandled { .. } => {}
            DataEvent::Eof => {
                break;
            }
            ev => {
                unreachable!("Unexpected data event: {:#?}", ev);
            }
        }
    }

    Ok(subset)
}

fn parse_replication<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
) -> Result<Vec<Sequence>, Error> {
    let mut replication = Vec::new();

    loop {
        match data_reader.read_event()? {
            DataEvent::ReplicationEnd => break,
            DataEvent::ReplicationItemStart => {
                let subset = parse_sequence(data_reader, tables)?;
                replication.push(subset);
            }
            ev => {
                unreachable!("Unexpected data event: {:#?}", ev);
            }
        }
    }

    Ok(replication)
}
