use std::{io::Read, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Builder, Int32Builder, StringBuilder, StructArray},
    buffer::OffsetBuffer,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};

use indexmap::IndexMap;

use crate::{
    tables::{TableBEntry, Tables},
    DataEvent, DataReader, DataSpec, Error, Value,
};

/// Unified column-oriented data structure
#[derive(Debug, Clone)]
pub enum ColumnData {
    Scalar {
        values: Vec<Value>,
        ty: DataType,
    },
    Struct {
        fields: IndexMap<String, ColumnData>,
    },
    List {
        offsets: Vec<i32>,
        items: Box<ColumnData>,
    },
}

/// Convert BUFR data to Arrow RecordBatch
/// 
/// This function combines the functionality of parse_data_as_columns and convert_to_arrow.
/// It reads BUFR data from a DataReader and converts it directly to an Arrow RecordBatch.
pub fn convert_to_arrow<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
    data_spec: &DataSpec,
) -> Result<RecordBatch, Error> {
    // Parse data into column-oriented structure
    let column_data = parse_data_as_columns(data_reader, tables, data_spec)?;
    
    // Convert to Arrow RecordBatch
    convert_column_data_to_arrow(column_data)
}

/// Parse data into column-oriented structure
fn parse_data_as_columns<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
    data_spec: &DataSpec,
) -> Result<IndexMap<String, ColumnData>, Error> {
    if data_spec.is_compressed {
        parse_compressed_as_columns(data_reader, tables, data_spec.number_of_subsets)
    } else {
        parse_non_compressed_as_columns(data_reader, tables)
    }
}

/// Parse compressed data (already column-oriented)
fn parse_compressed_as_columns<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
    num_subsets: u16,
) -> Result<IndexMap<String, ColumnData>, Error> {
    let mut columns = IndexMap::new();
    loop {
        match data_reader.read_event()? {
            DataEvent::CompressedStart => {
                parse_compressed_structure(data_reader, tables, &mut columns, num_subsets)?;
            }
            DataEvent::Eof => break,
            ev => {
                return Err(Error::Fatal(format!("Unexpected event: {:?}", ev)));
            }
        }
    }
    Ok(columns)
}

/// Context for tracking field name occurrences
#[derive(Default)]
struct FieldNameContext {
    element_name_counts: std::collections::HashMap<String, usize>,
    sequence_title_counts: std::collections::HashMap<String, usize>,
    replication_count: usize,
}

impl FieldNameContext {
    fn track_element(&mut self, element_name: &str) -> usize {
        let count = self
            .element_name_counts
            .entry(element_name.to_string())
            .or_insert(0);
        *count += 1;
        *count
    }

    fn track_sequence(&mut self, title: &str) -> usize {
        let count = self
            .sequence_title_counts
            .entry(title.to_string())
            .or_insert(0);
        *count += 1;
        *count
    }

    fn track_replication(&mut self) -> usize {
        self.replication_count += 1;
        self.replication_count
    }
}

/// Parse compressed structure recursively
fn parse_compressed_structure<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
    columns: &mut IndexMap<String, ColumnData>,
    num_subsets: u16,
) -> Result<(), Error> {
    let mut ctx = FieldNameContext::default();

    loop {
        match data_reader.read_event()? {
            DataEvent::CompressedData { xy, values, .. } => {
                let Some(b) = tables.table_b.get(&xy) else {
                    return Err(Error::Fatal(format!("Unknown data descriptor: {:#?}", xy)));
                };
                let count = ctx.track_element(b.element_name);
                let field_name = create_field_name(b, count);
                let ty = determine_arrow_type_from_table_b(b);

                columns.insert(field_name, ColumnData::Scalar { values, ty });
            }
            DataEvent::SequenceStart { xy, .. } => {
                let Some(d) = tables.table_d.get(&xy) else {
                    return Err(Error::Fatal(format!(
                        "Unknown sequence descriptor: {:#?}",
                        xy
                    )));
                };

                let count = ctx.track_sequence(d.title);
                let label = match count {
                    0 | 1 => d.title.to_string(),
                    _ => format!("{} ({})", d.title, count),
                };

                let mut sequence_fields = IndexMap::new();
                parse_compressed_structure(data_reader, tables, &mut sequence_fields, num_subsets)?;
                columns.insert(
                    label,
                    ColumnData::Struct {
                        fields: sequence_fields,
                    },
                );
            }
            DataEvent::ReplicationStart { .. } => {
                let rep_num = ctx.track_replication();
                let label = format!("replication:{}", rep_num);
                let replication_data =
                    parse_compressed_replication(data_reader, tables, num_subsets)?;
                columns.insert(label, replication_data);
            }
            DataEvent::SequenceEnd => break,
            DataEvent::OperatorHandled { .. } => {}
            DataEvent::Eof => break,
            ev => {
                return Err(Error::Fatal(format!(
                    "Unexpected event in compressed structure: {:?}",
                    ev
                )));
            }
        }
    }

    Ok(())
}

/// Parse compressed replication with offset tracking
fn parse_compressed_replication<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
    num_subsets: u16,
) -> Result<ColumnData, Error> {
    // For compressed data, we need to track repetition counts per subset
    let mut all_item_data = Vec::new();

    // Read all replication items
    loop {
        match data_reader.read_event()? {
            DataEvent::ReplicationItemStart => {
                let mut item_fields = IndexMap::new();
                parse_compressed_replication_item(
                    data_reader,
                    tables,
                    &mut item_fields,
                    num_subsets,
                )?;
                all_item_data.push(item_fields);
            }
            DataEvent::ReplicationEnd => break,
            ev => {
                return Err(Error::Fatal(format!(
                    "Unexpected event in compressed replication: {:?}",
                    ev
                )));
            }
        }
    }

    // Check if we have delayed replication factor (variable repetition counts)
    // For now, assume fixed repetition count for all subsets
    let items_per_subset = all_item_data.len() / num_subsets as usize;

    // Build offsets for fixed repetition count
    let mut offsets = vec![0i32];
    offsets.extend((1..=num_subsets).map(|i| i as i32 * items_per_subset as i32));

    // Merge all item data into a single structure
    let merged_items = merge_replication_items(all_item_data)?;

    Ok(ColumnData::List {
        offsets,
        items: Box::new(ColumnData::Struct {
            fields: merged_items,
        }),
    })
}

/// Parse compressed replication item (handles ReplicationItemEnd)
fn parse_compressed_replication_item<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
    columns: &mut IndexMap<String, ColumnData>,
    num_subsets: u16,
) -> Result<(), Error> {
    let mut ctx = FieldNameContext::default();

    loop {
        match data_reader.read_event()? {
            DataEvent::CompressedData { xy, values, .. } => {
                let Some(b) = tables.table_b.get(&xy) else {
                    return Err(Error::Fatal(format!("Unknown data descriptor: {:#?}", xy)));
                };

                let count = ctx.track_element(b.element_name);
                let field_name = create_field_name(b, count);
                let ty = determine_arrow_type_from_table_b(b);

                columns.insert(field_name, ColumnData::Scalar { values, ty });
            }
            DataEvent::SequenceStart { xy, .. } => {
                let Some(d) = tables.table_d.get(&xy) else {
                    return Err(Error::Fatal(format!(
                        "Unknown sequence descriptor: {:#?}",
                        xy
                    )));
                };

                let count = ctx.track_sequence(d.title);
                let label = match count {
                    0 | 1 => d.title.to_string(),
                    _ => format!("{} ({})", d.title, count),
                };

                let mut sequence_fields = IndexMap::new();
                parse_compressed_structure(data_reader, tables, &mut sequence_fields, num_subsets)?;
                columns.insert(
                    label,
                    ColumnData::Struct {
                        fields: sequence_fields,
                    },
                );
            }
            DataEvent::ReplicationStart { .. } => {
                let rep_num = ctx.track_replication();
                let label = format!("replication:{}", rep_num);
                let replication_data =
                    parse_compressed_replication(data_reader, tables, num_subsets)?;
                columns.insert(label, replication_data);
            }
            DataEvent::ReplicationItemEnd => break,
            DataEvent::OperatorHandled { .. } => {}
            ev => {
                return Err(Error::Fatal(format!(
                    "Unexpected event in compressed replication item: {:?}",
                    ev
                )));
            }
        }
    }

    Ok(())
}

/// Parse non-compressed data and convert to column-oriented structure
fn parse_non_compressed_as_columns<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
) -> Result<IndexMap<String, ColumnData>, Error> {
    // First pass: collect one subset to determine structure
    let first_subset = match data_reader.read_event()? {
        DataEvent::SubsetStart(_) => parse_subset(data_reader, tables)?,
        DataEvent::Eof => return Ok(IndexMap::new()),
        ev => return Err(Error::Fatal(format!("Unexpected event: {:?}", ev))),
    };

    // Initialize columns based on first subset structure with proper types from tables
    let mut columns = initialize_columns_from_subset(&first_subset)?;

    // Add first subset data to columns
    add_subset_to_columns(&first_subset, &mut columns)?;

    // Process remaining subsets
    loop {
        match data_reader.read_event()? {
            DataEvent::SubsetStart(_) => {
                let subset = parse_subset(data_reader, tables)?;
                add_subset_to_columns(&subset, &mut columns)?;
            }
            DataEvent::Eof => break,
            ev => {
                return Err(Error::Fatal(format!("Unexpected event: {:?}", ev)));
            }
        }
    }

    // Convert builders to final column data
    Ok(columns
        .into_iter()
        .map(|(k, v)| (k, v.into_column_data()))
        .collect())
}

/// Mutable column data for building
enum ColumnDataBuilder {
    Scalar {
        values: Vec<Value>,
        ty: DataType,
    },
    Struct {
        fields: IndexMap<String, ColumnDataBuilder>,
    },
    List {
        offsets: Vec<i32>,
        items: Box<ColumnDataBuilder>,
    },
}

impl ColumnDataBuilder {
    fn into_column_data(self) -> ColumnData {
        match self {
            ColumnDataBuilder::Scalar {
                values,
                ty: data_type,
            } => ColumnData::Scalar {
                values,
                ty: data_type,
            },
            ColumnDataBuilder::Struct { fields } => ColumnData::Struct {
                fields: fields
                    .into_iter()
                    .map(|(k, v)| (k, v.into_column_data()))
                    .collect(),
            },
            ColumnDataBuilder::List { offsets, items } => ColumnData::List {
                offsets,
                items: Box::new(items.into_column_data()),
            },
        }
    }
}

/// Initialize column builders from first subset
fn initialize_columns_from_subset(
    subset: &IndexMap<String, RowValue>,
) -> Result<IndexMap<String, ColumnDataBuilder>, Error> {
    subset
        .iter()
        .map(|(name, value)| {
            let builder = match value {
                RowValue::Scalar(_, b) => {
                    let data_type = determine_arrow_type_from_table_b(b);
                    ColumnDataBuilder::Scalar {
                        values: Vec::new(),
                        ty: data_type,
                    }
                }
                RowValue::Struct(fields) => ColumnDataBuilder::Struct {
                    fields: initialize_columns_from_subset(fields)?,
                },
                RowValue::List(items) => {
                    let item_builder = if items.is_empty() {
                        ColumnDataBuilder::Struct {
                            fields: IndexMap::new(),
                        }
                    } else {
                        ColumnDataBuilder::Struct {
                            fields: initialize_columns_from_subset(&items[0])?,
                        }
                    };
                    ColumnDataBuilder::List {
                        offsets: vec![0],
                        items: Box::new(item_builder),
                    }
                }
            };
            Ok((name.clone(), builder))
        })
        .collect()
}

/// Add subset data to column builders
fn add_subset_to_columns(
    subset: &IndexMap<String, RowValue>,
    columns: &mut IndexMap<String, ColumnDataBuilder>,
) -> Result<(), Error> {
    for (name, value) in subset {
        if let Some(column) = columns.get_mut(name) {
            add_value_to_column(value, column)?;
        }
    }
    Ok(())
}

/// Add a single value to a column builder
fn add_value_to_column(value: &RowValue, column: &mut ColumnDataBuilder) -> Result<(), Error> {
    match (value, column) {
        (RowValue::Scalar(v, _), ColumnDataBuilder::Scalar { values, .. }) => {
            values.push(v.clone());
        }
        (RowValue::Struct(fields), ColumnDataBuilder::Struct { fields: col_fields }) => {
            for (name, val) in fields {
                if let Some(col) = col_fields.get_mut(name) {
                    add_value_to_column(val, col)?;
                }
            }
        }
        (
            RowValue::List(items),
            ColumnDataBuilder::List {
                offsets,
                items: col_items,
            },
        ) => {
            let last_offset = *offsets.last().unwrap();
            offsets.push(last_offset + items.len() as i32);

            // Add each item to the items column
            for item in items {
                if let ColumnDataBuilder::Struct { fields } = &mut **col_items {
                    for (name, val) in item {
                        if let Some(col) = fields.get_mut(name) {
                            add_value_to_column(val, col)?;
                        }
                    }
                }
            }
        }
        _ => {
            return Err(Error::Fatal(
                "Type mismatch when adding to column".to_string(),
            ));
        }
    }
    Ok(())
}

/// Intermediate row-oriented data structure for non-compressed parsing
#[derive(Debug, Clone)]
enum RowValue {
    Scalar(Value, &'static TableBEntry),
    Struct(IndexMap<String, RowValue>),
    List(Vec<IndexMap<String, RowValue>>),
}

/// Parse a single subset in row format
fn parse_subset<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
) -> Result<IndexMap<String, RowValue>, Error> {
    let mut subset = IndexMap::new();
    let mut ctx = FieldNameContext::default();

    loop {
        match data_reader.read_event()? {
            DataEvent::SubsetEnd => break,
            DataEvent::Data { value, xy, .. } => {
                let Some(b) = tables.table_b.get(&xy) else {
                    return Err(Error::Fatal(format!("Unknown data descriptor: {:#?}", xy)));
                };
                let count = ctx.track_element(b.element_name);
                let label = create_field_name(b, count);
                subset.insert(label, RowValue::Scalar(value, b));
            }
            DataEvent::SequenceStart { xy, .. } => {
                let Some(d) = tables.table_d.get(&xy) else {
                    return Err(Error::Fatal(format!(
                        "Unknown sequence descriptor: {:#?}",
                        xy
                    )));
                };

                let count = ctx.track_sequence(d.title);
                let label = match count {
                    0 | 1 => d.title.to_string(),
                    _ => format!("{} ({})", d.title, count),
                };

                let sequence = parse_sequence(data_reader, tables)?;
                subset.insert(label, RowValue::Struct(sequence));
            }
            DataEvent::ReplicationStart { .. } => {
                let rep_num = ctx.track_replication();
                let label = format!("replication:{}", rep_num);
                let replication = parse_replication(data_reader, tables)?;
                subset.insert(label, RowValue::List(replication));
            }
            DataEvent::OperatorHandled { .. } => {}
            ev => {
                return Err(Error::Fatal(format!(
                    "Unexpected event in subset: {:?}",
                    ev
                )));
            }
        }
    }

    Ok(subset)
}

/// Parse sequence in row format
fn parse_sequence<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
) -> Result<IndexMap<String, RowValue>, Error> {
    let mut sequence = IndexMap::new();
    let mut ctx = FieldNameContext::default();

    loop {
        match data_reader.read_event()? {
            DataEvent::SequenceEnd | DataEvent::ReplicationItemEnd => break,
            DataEvent::Data { value, xy, .. } => {
                let Some(b) = tables.table_b.get(&xy) else {
                    return Err(Error::Fatal(format!("Unknown data descriptor: {:#?}", xy)));
                };
                let count = ctx.track_element(b.element_name);
                let label = create_field_name(b, count);
                sequence.insert(label, RowValue::Scalar(value, b));
            }
            DataEvent::SequenceStart { xy, .. } => {
                let Some(d) = tables.table_d.get(&xy) else {
                    return Err(Error::Fatal(format!(
                        "Unknown sequence descriptor: {:#?}",
                        xy
                    )));
                };

                let count = ctx.track_sequence(d.title);
                let label = match count {
                    0 | 1 => d.title.to_string(),
                    _ => format!("{} ({})", d.title, count),
                };

                let nested = parse_sequence(data_reader, tables)?;
                sequence.insert(label, RowValue::Struct(nested));
            }
            DataEvent::ReplicationStart { .. } => {
                let rep_num = ctx.track_replication();
                let label = format!("replication:{}", rep_num);
                let replication = parse_replication(data_reader, tables)?;
                sequence.insert(label, RowValue::List(replication));
            }
            DataEvent::OperatorHandled { .. } => {}
            ev => {
                return Err(Error::Fatal(format!(
                    "Unexpected event in sequence: {:?}",
                    ev
                )));
            }
        }
    }

    Ok(sequence)
}

/// Parse replication in row format
fn parse_replication<R: Read>(
    data_reader: &mut DataReader<'_, R>,
    tables: &Tables,
) -> Result<Vec<IndexMap<String, RowValue>>, Error> {
    let mut replication = Vec::new();
    loop {
        match data_reader.read_event()? {
            DataEvent::ReplicationEnd => break,
            DataEvent::ReplicationItemStart => {
                let item = parse_sequence(data_reader, tables)?;
                replication.push(item);
            }
            ev => {
                return Err(Error::Fatal(format!(
                    "Unexpected event in replication: {:?}",
                    ev
                )));
            }
        }
    }
    Ok(replication)
}

/// Convert column data to Arrow RecordBatch
fn convert_column_data_to_arrow(columns: IndexMap<String, ColumnData>) -> Result<RecordBatch, Error> {
    let (fields, arrays): (Vec<_>, Vec<_>) = columns
        .into_iter()
        .filter_map(|(name, column)| {
            // Skip empty structs as Parquet doesn't support them
            if is_empty_struct(&column) {
                None
            } else {
                Some(build_arrow_array(&name, column))
            }
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .unzip();
    let schema = Arc::new(Schema::new(fields));
    RecordBatch::try_new(schema, arrays)
        .map_err(|e| Error::Fatal(format!("Failed to create RecordBatch: {}", e)))
}

/// Check if a column is an empty struct or contains empty structs
fn is_empty_struct(column: &ColumnData) -> bool {
    match column {
        ColumnData::Struct { fields } => fields.is_empty(),
        ColumnData::List { items, .. } => is_empty_struct(items),
        _ => false,
    }
}

/// Build Arrow array from column data
fn build_arrow_array(field_name: &str, column: ColumnData) -> Result<(Field, ArrayRef), Error> {
    match column {
        ColumnData::Scalar {
            values,
            ty: data_type,
        } => build_scalar_array(field_name, values, data_type),
        ColumnData::Struct { fields } => {
            if fields.is_empty() {
                // Handle empty struct case
                let struct_array = StructArray::new_empty_fields(0, None);
                Ok((
                    Field::new(
                        field_name,
                        DataType::Struct(arrow::datatypes::Fields::empty()),
                        true,
                    ),
                    Arc::new(struct_array),
                ))
            } else {
                let (sub_fields, sub_arrays): (Vec<_>, Vec<_>) = fields
                    .into_iter()
                    .map(|(name, col)| build_arrow_array(&name, col))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .unzip();

                let struct_array = StructArray::new(sub_fields.clone().into(), sub_arrays, None);
                Ok((
                    Field::new(field_name, DataType::Struct(sub_fields.into()), true),
                    Arc::new(struct_array),
                ))
            }
        }
        ColumnData::List { offsets, items } => {
            let (item_field, item_array) = match *items {
                ColumnData::Struct { fields } => {
                    if fields.is_empty() {
                        // Calculate the length from offsets
                        let len = offsets.last().copied().unwrap_or(0) as usize;
                        let struct_array = StructArray::new_empty_fields(len, None);
                        (
                            Field::new(
                                "item",
                                DataType::Struct(arrow::datatypes::Fields::empty()),
                                true,
                            ),
                            Arc::new(struct_array) as ArrayRef,
                        )
                    } else {
                        let (sub_fields, sub_arrays): (Vec<_>, Vec<_>) = fields
                            .into_iter()
                            .map(|(name, col)| build_arrow_array(&name, col))
                            .collect::<Result<Vec<_>, _>>()?
                            .into_iter()
                            .unzip();

                        let struct_array =
                            StructArray::new(sub_fields.clone().into(), sub_arrays, None);
                        (
                            Field::new("item", DataType::Struct(sub_fields.into()), true),
                            Arc::new(struct_array) as ArrayRef,
                        )
                    }
                }
                _ => {
                    return Err(Error::Fatal("List items must be struct type".to_string()));
                }
            };

            let item_field_arc = Arc::new(item_field);
            let list_array = arrow::array::ListArray::try_new(
                item_field_arc.clone(),
                OffsetBuffer::new(offsets.into()),
                item_array,
                None,
            )
            .map_err(|e| Error::Fatal(format!("Failed to create list array: {}", e)))?;

            Ok((
                Field::new(field_name, DataType::List(item_field_arc), true),
                Arc::new(list_array),
            ))
        }
    }
}

/// Build scalar Arrow array
fn build_scalar_array(
    field_name: &str,
    values: Vec<Value>,
    data_type: DataType,
) -> Result<(Field, ArrayRef), Error> {
    match data_type {
        DataType::Utf8 => {
            let mut builder = StringBuilder::new();
            for value in values {
                match value {
                    crate::Value::String(s) => builder.append_value(s),
                    crate::Value::Missing => builder.append_null(),
                    _ => return Err(Error::Fatal("Type mismatch: expected string".to_string())),
                }
            }
            Ok((
                Field::new(field_name, DataType::Utf8, true),
                Arc::new(builder.finish()),
            ))
        }
        DataType::Int32 => {
            let mut builder = Int32Builder::new();
            for value in values {
                match value {
                    crate::Value::Integer(v) => builder.append_value(v),
                    crate::Value::Decimal(v, scale) => {
                        builder.append_value((v as f64 * 10f64.powi(scale as i32)) as i32)
                    }
                    crate::Value::Missing => builder.append_null(),
                    _ => return Err(Error::Fatal("Type mismatch: expected integer".to_string())),
                }
            }
            Ok((
                Field::new(field_name, DataType::Int32, true),
                Arc::new(builder.finish()),
            ))
        }
        DataType::Float64 => {
            let mut builder = Float64Builder::new();
            for value in values {
                match value {
                    crate::Value::Integer(v) => builder.append_value(v as f64),
                    crate::Value::Decimal(v, scale) => {
                        builder.append_value(v as f64 * 10f64.powi(scale as i32))
                    }
                    crate::Value::Missing => builder.append_null(),
                    _ => return Err(Error::Fatal("Type mismatch: expected numeric".to_string())),
                }
            }
            Ok((
                Field::new(field_name, DataType::Float64, true),
                Arc::new(builder.finish()),
            ))
        }
        DataType::Null => Ok((
            Field::new(field_name, DataType::Null, true),
            Arc::new(arrow::array::NullArray::new(values.len())),
        )),
        _ => Err(Error::Fatal(format!(
            "Unsupported data type: {:?}",
            data_type
        ))),
    }
}

/// Helper functions
fn create_field_name(b: &TableBEntry, count: usize) -> String {
    match b.unit {
        "Numeric" => match count {
            0 | 1 => b.element_name.to_string(),
            _ => format!("{} ({})", b.element_name, count),
        },
        _ => match count {
            0 | 1 => format!("{} [{}]", b.element_name, b.unit),
            _ => format!("{} [{}] ({})", b.element_name, b.unit, count),
        },
    }
}

fn determine_arrow_type_from_table_b(entry: &TableBEntry) -> DataType {
    match entry.unit {
        "CCITT IA5" => DataType::Utf8,
        "Code table" | "Flag table" => DataType::Int32,
        _ if entry.scale == 0 => DataType::Int32,
        _ if entry.scale < 0 => DataType::Float64,
        _ => DataType::Int32,
    }
}

fn merge_replication_items(
    items: Vec<IndexMap<String, ColumnData>>,
) -> Result<IndexMap<String, ColumnData>, Error> {
    if items.is_empty() {
        return Ok(IndexMap::new());
    }

    // Get field names from first item
    let field_names: Vec<String> = items[0].keys().cloned().collect();

    field_names
        .into_iter()
        .map(|field_name| {
            // Collect values for this field from all items
            let mut all_values = Vec::new();
            let mut data_type = DataType::Null;

            for item in items.iter() {
                if let Some(column_data) = item.get(&field_name) {
                    match column_data {
                        ColumnData::Scalar { values, ty: dt } => {
                            all_values.extend_from_slice(values);
                            if matches!(data_type, DataType::Null) {
                                data_type = dt.clone();
                            }
                        }
                        _ => {
                            return Err(Error::Fatal(
                                "Nested structures in replication not yet supported".to_string(),
                            ));
                        }
                    }
                }
            }

            Ok((
                field_name,
                ColumnData::Scalar {
                    values: all_values,
                    ty: data_type,
                },
            ))
        })
        .collect()
}