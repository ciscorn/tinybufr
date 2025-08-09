use std::io::Read;

use tinybufr::{DataEvent, DataReader, DataSpec, HeaderSections, Tables, ensure_end_section};

/// Test that a file can be fully read without errors and matching start/end markers.
pub fn test_full_read(mut reader: impl Read, tables: &Tables) {
    // Parse header sections
    let header = HeaderSections::read(&mut reader).unwrap();
    println!("{}", serde_json::to_string_pretty(&header).unwrap());

    // Parse data section
    let data_spec =
        DataSpec::from_data_description(&header.data_description_section, &tables).unwrap();
    let mut data_reader = DataReader::new(&mut reader, &data_spec).unwrap();

    let mut subset_counter = 0;
    let mut sequence_counter = 0;
    let mut replication_counter = 0;
    let mut replication_item_counter = 0;

    loop {
        match data_reader.read_event() {
            Ok(DataEvent::SubsetStart { .. }) => {
                subset_counter += 1;
            }
            Ok(DataEvent::SubsetEnd) => {
                subset_counter -= 1;
                assert_eq!(sequence_counter, 0);
                assert_eq!(replication_item_counter, 0);
            }
            Ok(DataEvent::SequenceStart { .. }) => {
                sequence_counter += 1;
            }
            Ok(DataEvent::SequenceEnd) => {
                sequence_counter -= 1;
                assert_eq!(replication_counter, 0);
            }
            Ok(DataEvent::ReplicationStart { .. }) => {
                println!("Replication start");
                replication_counter += 1;
            }
            Ok(DataEvent::ReplicationItemStart) => {
                println!("Replication item start");
                replication_item_counter += 1;
            }
            Ok(DataEvent::ReplicationItemEnd) => {
                println!("Replication item end");
                replication_item_counter -= 1;
            }
            Ok(DataEvent::ReplicationEnd) => {
                println!("Replication end");
                replication_counter -= 1;
            }
            Ok(DataEvent::Data { .. }) => {
                println!("Data");
            }
            Ok(DataEvent::CompressedData { .. }) => {}
            Ok(DataEvent::Eof) => {
                assert_eq!(subset_counter, 0);
                break;
            }
            Ok(_) => {}
            Err(e) => {
                panic!("Error: {:?}", e);
            }
        }
    }

    ensure_end_section(header.indicator_section.edition_number, &mut reader).unwrap();
}
