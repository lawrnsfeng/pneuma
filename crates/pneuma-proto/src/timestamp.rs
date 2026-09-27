//! [`IsoTimestamp`] — a UTC instant formatted exactly as original writes it.
//!
//! Ported from the `IsoDatetime` alias at
//! the original:
//!
//! # Why this is not just `DateTime<Utc>`
//!
//! chrono's derived serialization would produce a different string for the same
//! instant, and this is the one field whose bytes reach a consumer outside the
//! system — the customer's backend, over its own `topic_event`.
//!
//! Checked against the running interpreter rather than assumed. The
//! `PlainSerializer` fires in `model_dump()` (reference mode), so the value is
//! already a `str` before orjson sees it, and a raw `datetime` down the same
//! path formats identically:
//!
//! ```text
//! model_dump type : str -> '2026-08-28T04:26:45.671363+00:00'
//! orjson of dump  : {"timestamp":"2026-08-28T04:26:45.671363+00:00"}
//! raw datetime    : {"t":"2026-08-28T04:26:45.671414+00:00"}
//! ```
//!
//! So the target is: **`+00:00`, never `Z`**, and **six fractional digits,
//! omitted entirely when the microseconds are zero** — `datetime.isoformat()`
//! writes either 0 or 6, never 3.
//!
//! That last rule rules out both obvious chrono calls.
//! [`SecondsFormat::Micros`] always writes six digits, so it is wrong on a
//! whole second. [`SecondsFormat::AutoSi`] picks 0, 3, 6, or 9, so it is wrong
//! whenever the microseconds are a non-zero multiple of 1000 — original writes
//! `.671000` where `AutoSi` writes `.671`. Exact parity needs the choice made
//! per value, which is what [`IsoTimestamp::to_reference_isoformat`] does.
//!
//! # Reading is deliberately wider than writing
//!
//! Decoding accepts any RFC 3339 timestamp, `Z` included, and converts to UTC.
//! Being strict on the way out and lenient on the way in is the right asymmetry
//! for a wire type: this crate controls what it emits and does not control what
//! arrives.

use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A UTC instant that serializes the way `datetime.isoformat()` does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IsoTimestamp(DateTime<Utc>);

impl IsoTimestamp {
    /// The current instant, truncated to microseconds.
    ///
    /// Mirrors `Field(default_factory=lambda: datetime.now(UTC))`.
    pub fn now() -> Self {
        Self::from(Utc::now())
    }

    /// Discards precision finer than a microsecond.
    ///
    /// This is what makes the round trip total. `Utc::now()` has nanosecond
    /// resolution on Linux, the wire format has microsecond resolution, and
    /// `PartialEq`/`Ord`/`Hash` are derived over the whole instant — so without
    /// this a freshly stamped value compares **unequal** to the same value
    /// after a publish-and-echo hop, silently breaking anything that correlates
    /// on it. original cannot represent sub-microsecond precision at all, so
    /// discarding it is the faithful reading rather than a lossy shortcut.
    ///
    /// Written with `with_nanosecond` rather than `trunc_subsecs` so a leap
    /// second, which chrono encodes as `nanos >= 1_000_000_000`, keeps its
    /// marker: truncating `1_000_000_000` yields `1_000_000_000`.
    fn truncate_to_micros(value: DateTime<Utc>) -> DateTime<Utc> {
        let nanos = value.timestamp_subsec_nanos();
        value
            .with_nanosecond(nanos / 1_000 * 1_000)
            .unwrap_or(value)
    }

    /// The underlying instant.
    pub fn as_datetime(self) -> DateTime<Utc> {
        self.0
    }

    /// Formats exactly as the original's `datetime.isoformat()` on an aware UTC
    /// value: `+00:00` rather than `Z`, and six fractional digits or none.
    pub fn to_reference_isoformat(self) -> String {
        // Guarded on nanos modulo a second, not on `timestamp_subsec_micros`.
        // chrono encodes a leap second as `nanos >= 1_000_000_000`, so
        // `timestamp_subsec_micros` reports 1_000_000 there — non-zero, which
        // would select `Micros` — while the formatter subtracts a whole second
        // before writing the fraction and emits `.000000`. That is a six-digit
        // all-zero fraction, exactly what this function exists to avoid.
        let format = if self
            .0
            .timestamp_subsec_nanos()
            .is_multiple_of(1_000_000_000)
        {
            SecondsFormat::Secs
        } else {
            SecondsFormat::Micros
        };
        // `use_z = false` is what yields `+00:00` instead of `Z`.
        self.0.to_rfc3339_opts(format, false)
    }
}

impl Default for IsoTimestamp {
    fn default() -> Self {
        Self::now()
    }
}

impl From<DateTime<Utc>> for IsoTimestamp {
    /// Truncates to microseconds, so every construction path yields a value
    /// that survives its own wire format — see `truncate_to_micros`.
    fn from(value: DateTime<Utc>) -> Self {
        IsoTimestamp(Self::truncate_to_micros(value))
    }
}

impl From<IsoTimestamp> for DateTime<Utc> {
    fn from(value: IsoTimestamp) -> Self {
        value.0
    }
}

impl Serialize for IsoTimestamp {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.to_reference_isoformat())
    }
}

impl<'de> Deserialize<'de> for IsoTimestamp {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&raw)
            // Through `From`, not the tuple constructor, so an inbound
            // sub-microsecond timestamp is truncated like any other. the original's
            // `time.RFC3339Nano` emits nanoseconds, and without this a decoded
            // value would not equal itself after being re-encoded.
            .map(|parsed| IsoTimestamp::from(parsed.with_timezone(&Utc)))
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32, micros: u32) -> Option<IsoTimestamp> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .single()
            .map(|t| IsoTimestamp(t + chrono::Duration::microseconds(i64::from(micros))))
    }

    /// A civil instant: year, month, day, hour, minute, second, microsecond.
    type Parts = (i32, u32, u32, u32, u32, u32, u32);

    /// Golden values produced by the real interpreter:
    /// `datetime(*case, tzinfo=UTC).isoformat()`.
    const GOLDEN: [(Parts, &str); 5] = [
        (
            (2026, 8, 28, 4, 26, 45, 671363),
            "2026-08-28T04:26:45.671363+00:00",
        ),
        ((2026, 8, 28, 4, 26, 45, 0), "2026-08-28T04:26:45+00:00"),
        (
            (2026, 8, 28, 4, 26, 45, 671000),
            "2026-08-28T04:26:45.671000+00:00",
        ),
        ((2026, 1, 1, 0, 0, 0, 1), "2026-01-01T00:00:00.000001+00:00"),
        ((1970, 1, 1, 0, 0, 0, 0), "1970-01-01T00:00:00+00:00"),
    ];

    #[test]
    fn matches_reference_isoformat_byte_for_byte() -> Result<(), serde_json::Error> {
        for ((y, mo, d, h, mi, s, micros), expected) in GOLDEN {
            let Some(stamp) = at(y, mo, d, h, mi, s, micros) else {
                panic!("{y}-{mo}-{d} is not a valid instant");
            };
            assert_eq!(stamp.to_reference_isoformat(), expected);
            // And through serde, which is how it actually reaches the wire.
            assert_eq!(serde_json::to_string(&stamp)?, format!("\"{expected}\""));
        }
        Ok(())
    }

    #[test]
    fn a_whole_second_omits_the_fraction_entirely() {
        // SecondsFormat::Micros would write `.000000` here.
        let Some(stamp) = at(2026, 8, 28, 4, 26, 45, 0) else {
            panic!("invalid instant");
        };
        let text = stamp.to_reference_isoformat();
        assert!(!text.contains('.'), "{text}");
        assert!(text.ends_with("+00:00"), "{text}");
    }

    #[test]
    fn a_millisecond_multiple_still_writes_six_digits() {
        // The case that rules out SecondsFormat::AutoSi, which would write
        // `.671` where original writes `.671000`.
        let Some(stamp) = at(2026, 8, 28, 4, 26, 45, 671000) else {
            panic!("invalid instant");
        };
        assert!(stamp.to_reference_isoformat().contains(".671000"));
    }

    #[test]
    fn the_offset_is_never_written_as_z() -> Result<(), serde_json::Error> {
        // the original's isoformat on an aware UTC value writes +00:00.
        let stamp = IsoTimestamp::now();
        let text = serde_json::to_string(&stamp)?;
        assert!(text.ends_with("+00:00\""), "{text}");
        assert!(!text.contains('Z'), "{text}");
        Ok(())
    }

    #[test]
    fn reading_accepts_the_z_form_it_does_not_write() -> Result<(), serde_json::Error> {
        // Deliberately wider on the way in: this crate controls what it emits
        // and does not control what arrives.
        let z: IsoTimestamp = serde_json::from_str("\"2026-08-28T04:26:45.671363Z\"")?;
        let offset: IsoTimestamp = serde_json::from_str("\"2026-08-28T04:26:45.671363+00:00\"")?;
        assert_eq!(z, offset);
        // ...but it still writes the +00:00 form.
        assert_eq!(
            serde_json::to_string(&z)?,
            "\"2026-08-28T04:26:45.671363+00:00\""
        );
        Ok(())
    }

    #[test]
    fn a_non_utc_offset_is_converted_rather_than_rejected() -> Result<(), serde_json::Error> {
        let shifted: IsoTimestamp = serde_json::from_str("\"2026-08-28T06:26:45.671363+02:00\"")?;
        assert_eq!(
            serde_json::to_string(&shifted)?,
            "\"2026-08-28T04:26:45.671363+00:00\""
        );
        Ok(())
    }

    #[test]
    fn round_trips_through_text() -> Result<(), serde_json::Error> {
        for ((y, mo, d, h, mi, s, micros), _) in GOLDEN {
            let Some(stamp) = at(y, mo, d, h, mi, s, micros) else {
                panic!("invalid instant");
            };
            let text = serde_json::to_string(&stamp)?;
            assert_eq!(serde_json::from_str::<IsoTimestamp>(&text)?, stamp);
        }
        Ok(())
    }

    #[test]
    fn an_inbound_nanosecond_timestamp_is_truncated_on_decode() -> Result<(), serde_json::Error> {
        // Truncating only in `From` left this door open: a producer emitting
        // RFC 3339 nanoseconds -- the original's time.RFC3339Nano does -- yielded a value
        // that did not survive its own re-encoding.
        let first: IsoTimestamp = serde_json::from_str("\"2026-08-28T04:26:45.671363999+00:00\"")?;
        assert_eq!(
            first.to_reference_isoformat(),
            "2026-08-28T04:26:45.671363+00:00"
        );

        let text = serde_json::to_string(&first)?;
        let second: IsoTimestamp = serde_json::from_str(&text)?;
        assert_eq!(first, second, "decode must be idempotent through the wire");
        Ok(())
    }

    #[test]
    fn a_malformed_timestamp_is_rejected() {
        for bad in [
            "\"not a date\"",
            "\"2026-08-28\"",
            "\"2026-08-28T04:26:45\"",
            "7",
            "null",
        ] {
            assert!(
                serde_json::from_str::<IsoTimestamp>(bad).is_err(),
                "{bad} should be rejected"
            );
        }
    }

    #[test]
    fn converts_to_and_from_a_chrono_datetime() {
        let Some(stamp) = at(2026, 8, 28, 4, 26, 45, 1) else {
            panic!("invalid instant");
        };
        let inner: DateTime<Utc> = stamp.into();
        assert_eq!(IsoTimestamp::from(inner), stamp);
        assert_eq!(stamp.as_datetime(), inner);
    }

    #[test]
    fn ordering_is_by_instant() {
        // Asserted on fixed instants. `Utc::now()` reads the wall clock, which
        // NTP or an operator can step backwards, so ordering two `now()` calls
        // would be a flaky test of the clock rather than of this type.
        let (Some(earlier), Some(later)) =
            (at(2026, 8, 28, 4, 26, 45, 0), at(2026, 8, 28, 4, 26, 45, 1))
        else {
            panic!("invalid instants");
        };
        assert!(earlier < later);
        assert!(format!("{earlier:?}").contains("IsoTimestamp"));

        use std::collections::HashSet;
        let set: HashSet<_> = [earlier, earlier].into();
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn now_and_default_produce_a_value_that_survives_its_own_wire_format(
    ) -> Result<(), serde_json::Error> {
        // `Utc::now()` has nanosecond resolution on Linux and the wire format
        // has microsecond resolution, so without truncation a freshly stamped
        // value would not equal itself after a round trip.
        for stamp in [IsoTimestamp::now(), IsoTimestamp::default()] {
            let text = serde_json::to_string(&stamp)?;
            assert_eq!(serde_json::from_str::<IsoTimestamp>(&text)?, stamp);
            assert!(stamp
                .as_datetime()
                .timestamp_subsec_nanos()
                .is_multiple_of(1_000));
        }
        Ok(())
    }

    #[test]
    fn a_nanosecond_precise_instant_is_truncated_on_construction() -> Result<(), serde_json::Error>
    {
        let Some(base) = Utc.with_ymd_and_hms(2026, 8, 28, 4, 26, 45).single() else {
            panic!("invalid instant");
        };
        let precise = base + chrono::Duration::nanoseconds(671_363_988);
        let stamp = IsoTimestamp::from(precise);

        assert_eq!(
            stamp.to_reference_isoformat(),
            "2026-08-28T04:26:45.671363+00:00"
        );
        let text = serde_json::to_string(&stamp)?;
        assert_eq!(serde_json::from_str::<IsoTimestamp>(&text)?, stamp);
        Ok(())
    }

    #[test]
    fn a_leap_second_omits_its_all_zero_fraction() {
        // chrono encodes a leap second as nanos >= 1_000_000_000, so
        // timestamp_subsec_micros reports 1_000_000 -- non-zero -- while the
        // formatter subtracts a whole second and writes `.000000`. Guarding on
        // micros emitted a six-digit all-zero fraction, which is the one thing
        // this function exists to avoid.
        let Some(day) = chrono::NaiveDate::from_ymd_opt(2016, 12, 31) else {
            panic!("bad date");
        };
        let Some(naive) = day.and_hms_nano_opt(23, 59, 59, 1_000_000_000) else {
            panic!("bad leap time");
        };
        let stamp = IsoTimestamp::from(naive.and_utc());

        assert_eq!(stamp.to_reference_isoformat(), "2016-12-31T23:59:60+00:00");
        // And the leap marker survives truncation.
        assert_eq!(stamp.as_datetime().timestamp_subsec_nanos(), 1_000_000_000);
    }
}
