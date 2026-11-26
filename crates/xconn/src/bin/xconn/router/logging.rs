use std::{fmt, io::IsTerminal};
use tracing::Level;
use tracing_subscriber::{
    Layer,
    filter::{LevelFilter, filter_fn},
    fmt::{format::Writer, format::debug_fn},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

use super::config::Result;

pub(super) const LIFECYCLE: &str = "xconn::router::lifecycle";

pub(super) fn init(level: Level) -> Result<()> {
    let ansi = std::io::stderr().is_terminal();
    let timer: fn(&mut Writer<'_>) -> fmt::Result = timestamp;
    let lifecycle = tracing_subscriber::fmt::layer()
        .with_timer(timer)
        .with_target(false)
        .with_ansi(ansi)
        .with_writer(std::io::stderr)
        .fmt_fields(debug_fn(|writer, field, value| {
            if field.name() == "message" {
                write!(writer, "{value:?}")?;
            }
            Ok(())
        }))
        .with_filter(filter_fn(|metadata| metadata.target() == LIFECYCLE));

    let diagnostics = tracing_subscriber::fmt::layer()
        .with_timer(timer)
        .with_ansi(ansi)
        .with_writer(std::io::stderr)
        .with_filter(filter_fn(|metadata| metadata.target() != LIFECYCLE));

    tracing_subscriber::registry()
        .with(LevelFilter::from_level(level))
        .with(lifecycle)
        .with(diagnostics)
        .try_init()?;
    Ok(())
}

fn timestamp(writer: &mut Writer<'_>) -> fmt::Result {
    let now = time::OffsetDateTime::now_utc();
    let (year, month, day) = now.to_calendar_date();
    let (hour, minute, second) = now.to_hms();
    let month = month as u8;
    write!(
        writer,
        "{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}Z"
    )
}
