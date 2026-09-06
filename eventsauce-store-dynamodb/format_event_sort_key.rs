pub fn format_event_sort_key(sequence_number: u32) -> String {
    format!("{}{:0width$}", "EVENT#", sequence_number, width = 10)
}
