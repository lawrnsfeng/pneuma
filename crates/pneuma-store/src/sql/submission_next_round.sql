-- The next dispatch round, shared across every replica.
--
-- `nextval` on a sequence rather than a counter in the process, because
-- `select_batch` rotates its tie-break by this number: a counter that resets on
-- restart, or that each replica keeps its own copy of, leaves two dispatchers
-- sitting on the same low round and favouring the same tenant for ever.
SELECT nextval('dispatch_round')
