use uuid::Uuid;

pub fn format_event_key(stream_id: Uuid, sequence_number: u32) -> String {
    format!("{}{:010}", format_event_key_prefix(stream_id), sequence_number)
}

pub fn format_event_key_prefix(stream_id: Uuid) -> String {
    format!("{}#EVENT#", stream_id)
}
