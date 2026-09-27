-- Every queued submission, grouped by tenant and oldest first within a tenant.
--
-- The whole backlog, bounded by $1 per tenant rather than in total, because
-- `pneuma_fairness::select_batch` computes each flow's quota from what that
-- flow is holding: handing it a globally-truncated list would let a noisy
-- tenant's rows crowd a quiet one's out of the input entirely, and the
-- selection would then be provably fair over a sample that was not.
--
-- `ORDER BY` inside the window and again outside it: the first decides which
-- rows survive the per-tenant limit, the second decides the order they are
-- handed over in. Without the outer one the row order is whatever the plan
-- produced, and `select_batch` takes arrival order as given.
SELECT run_id, tenant_id, payload, enqueued_at
FROM (
    SELECT run_id, tenant_id, payload, enqueued_at,
           row_number() OVER (PARTITION BY tenant_id ORDER BY enqueued_at, run_id) AS rank
    FROM submission
    WHERE state = 'queued'
) ranked
WHERE rank <= $1
ORDER BY tenant_id, enqueued_at, run_id
