//! The JSON bodies the termination endpoints exchange.
//!
//! Field names and shapes mirror the read-API service's own termination model
//! exactly. These are wire types: renaming a field here breaks a peer.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// `POST /terminations` request body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CreateTerminationRequest {
    /// The job to cancel. Sent unencoded — it is a JSON string value, not a
    /// path segment.
    pub job_id: String,
}

/// A termination record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Termination {
    /// Server-assigned identifier. A real UUID: the
    /// `/terminations/{termination_id}` route extracts `Path<Uuid>`, so a
    /// malformed one is rejected before the handler runs.
    pub id: Uuid,
    /// The job this targets.
    pub job_id: String,
    /// Where it is in its lifecycle.
    pub status: TerminationStatus,
    /// The `run_id`s targeted for cancellation. JSONB on the server.
    pub run_ids: Vec<String>,
    /// When the record was created.
    pub created_at: DateTime<Utc>,
    /// When it was last modified.
    pub updated_at: DateTime<Utc>,
}

/// Lifecycle of a [`Termination`].
///
/// The gateway serialises these `snake_case` and backs them with a Postgres
/// enum of the same spelling, so the wire names are fixed twice over.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum TerminationStatus {
    /// Created, not yet acted on. The server's default.
    #[default]
    Pending,
    /// Cancellation in progress.
    Running,
    /// Cancellation complete.
    Finished,
}

/// Single-item wrapper — what `POST /terminations` returns under `data`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataResponse<T> {
    /// The item.
    pub data: T,
}

/// `{total, items}`.
///
/// One of three list envelopes the gateway uses, and **only**
/// `GET /terminations` returns this one.
/// The by-job endpoint
/// returns [`ListResponse`] instead.
///
/// Picking the wrong one is what produced the false headline defect retracted
/// in the defect notes — and an earlier version of this doc made the
/// same mistake, claiming this envelope for "the termination list endpoints"
/// generally. Both are modelled here so a caller does not have to guess.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ItemsResponse<T> {
    /// How many items exist.
    pub total: u64,
    /// The page of items.
    pub items: Vec<T>,
}

/// `{data: [...]}`.
///
/// What `GET /terminations/by-job/{job_id}` returns
/// — the read
/// counterpart of the endpoint this crate exists for. Note it carries no
/// `total`, so decoding it as [`ItemsResponse`] fails.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ListResponse<T> {
    /// The items.
    pub data: Vec<T>,
}

/// What a successful delete returns.
///
/// **`deleted_count` is never zero on the wire.** Both delete handlers turn
/// "nothing matched" into `404 NotFound` before constructing this body, so a
/// caller that only handles this type will treat the ordinary "already cleaned
/// up" case as a hard failure. The original client gets this right, tolerating
/// that 404 and returning `{}`.
///
/// An earlier version of this doc said zero was "a normal answer, not an
/// error", and a test pinned it. That was backwards, and it is the same
/// silent-cleanup-failure class the defect notes are about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeleteResponse {
    /// Rows removed. Always at least one: see the type's note.
    pub deleted_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_termination_round_trips_through_the_gateways_json() {
        // Shaped exactly as the gateway serialises it.
        let raw = serde_json::json!({
            "id": "6f1c9d2e-6a1b-4c3d-8e9f-0a1b2c3d4e5f",
            "job_id": "job-abc",
            "status": "pending",
            "run_ids": ["job-abc.run1", "job-abc.run2"],
            "created_at": "2026-08-28T10:11:12Z",
            "updated_at": "2026-08-28T10:11:13Z"
        });
        let Ok(termination) = serde_json::from_value::<Termination>(raw.clone()) else {
            panic!("should decode");
        };
        assert_eq!(termination.job_id, "job-abc");
        assert_eq!(termination.status, TerminationStatus::Pending);
        assert_eq!(termination.run_ids.len(), 2);
        assert_eq!(
            termination.id.to_string(),
            "6f1c9d2e-6a1b-4c3d-8e9f-0a1b2c3d4e5f"
        );

        let Ok(back) = serde_json::to_value(&termination) else {
            panic!("should encode");
        };
        assert_eq!(back["job_id"], raw["job_id"]);
        assert_eq!(back["status"], "pending");
        assert_eq!(back["run_ids"], raw["run_ids"]);
    }

    #[test]
    fn status_uses_the_gateways_snake_case_spelling() {
        // Backed by a Postgres enum of the same spelling, so these names are
        // fixed on the wire and in the database.
        for (status, wire) in [
            (TerminationStatus::Pending, "\"pending\""),
            (TerminationStatus::Running, "\"running\""),
            (TerminationStatus::Finished, "\"finished\""),
        ] {
            let Ok(encoded) = serde_json::to_string(&status) else {
                panic!("should encode");
            };
            assert_eq!(encoded, wire);
            let Ok(decoded) = serde_json::from_str::<TerminationStatus>(wire) else {
                panic!("should decode");
            };
            assert_eq!(decoded, status);
        }
        assert_eq!(TerminationStatus::default(), TerminationStatus::Pending);
    }

    #[test]
    fn a_malformed_uuid_is_rejected_rather_than_carried() {
        // The `/terminations/{termination_id}` route extracts Path<Uuid>, so an
        // id that is not a UUID never reaches a handler. Modelling it as a Uuid
        // keeps that property on this side too.
        let raw = serde_json::json!({
            "id": "runs",
            "job_id": "j", "status": "pending", "run_ids": [],
            "created_at": "2026-08-28T10:11:12Z",
            "updated_at": "2026-08-28T10:11:12Z"
        });
        assert!(serde_json::from_value::<Termination>(raw).is_err());
    }

    #[test]
    fn the_response_envelopes_match_the_gateways_three_shapes() {
        // Picking the wrong envelope is what produced the false headline defect
        // retracted in the defect notes, so each is pinned by name.
        let Ok(items) = serde_json::from_str::<ItemsResponse<DeleteResponse>>(
            r#"{"total":2,"items":[{"deleted_count":1},{"deleted_count":4}]}"#,
        ) else {
            panic!("should decode");
        };
        assert_eq!(items.total, 2);
        assert_eq!(items.items[1].deleted_count, 4);

        let Ok(data) =
            serde_json::from_str::<DataResponse<DeleteResponse>>(r#"{"data":{"deleted_count":7}}"#)
        else {
            panic!("should decode");
        };
        assert_eq!(data.data.deleted_count, 7);

        // The by-job GET uses the OTHER list envelope -- no `total`.
        let Ok(list) = serde_json::from_str::<ListResponse<DeleteResponse>>(
            r#"{"data":[{"deleted_count":1}]}"#,
        ) else {
            panic!("should decode");
        };
        assert_eq!(list.data[0].deleted_count, 1);

        // And the two are genuinely not interchangeable, which is the whole
        // reason both are modelled.
        assert!(
            serde_json::from_str::<ItemsResponse<DeleteResponse>>(r#"{"data":[]}"#).is_err(),
            "ItemsResponse requires `total`"
        );
    }

    #[test]
    fn a_delete_never_reports_zero_on_the_wire() {
        // Both delete handlers return 404 when nothing matched, so
        // {"deleted_count":0} is not a response the gateway can produce. The
        // type still parses it -- being lenient about a body costs nothing --
        // but a caller must handle the 404, which is where "already cleaned up"
        // actually arrives. An earlier version of this test asserted the
        // opposite as intended behaviour.
        let Ok(zero) = serde_json::from_str::<DeleteResponse>(r#"{"deleted_count":0}"#) else {
            panic!("a zero body still parses; it just never arrives");
        };
        assert_eq!(zero.deleted_count, 0);
    }

    #[test]
    fn the_create_request_sends_the_id_raw() {
        let request = CreateTerminationRequest {
            job_id: "with/slash".to_owned(),
        };
        let Ok(body) = serde_json::to_string(&request) else {
            panic!("should encode");
        };
        // A JSON string value, not a path segment: no percent-encoding here.
        assert_eq!(body, r#"{"job_id":"with/slash"}"#);
        let Ok(back) = serde_json::from_str::<CreateTerminationRequest>(&body) else {
            panic!("should decode");
        };
        assert_eq!(back, request);
    }
}
