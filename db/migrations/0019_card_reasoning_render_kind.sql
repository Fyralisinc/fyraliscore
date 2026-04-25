-- 0019_card_reasoning_render_kind.sql
--
-- Adds 'card_reasoning' to view_render_costs.render_kind CHECK set.
-- The Gate-4b rev-3 card-reasoning endpoint emits this kind when GRT
-- calls /rendering/card-reasoning to compose the expanded-drawer body.
-- Without this, the INSERT into view_render_costs fails and the
-- rendering.cost_record_failed warning fires on every card drawer render.

BEGIN;

ALTER TABLE view_render_costs
    DROP CONSTRAINT IF EXISTS view_render_costs_render_kind_check;

ALTER TABLE view_render_costs
    ADD CONSTRAINT view_render_costs_render_kind_check
    CHECK (render_kind = ANY (ARRAY[
        'greeting'::text,
        'card_observation'::text,
        'card_decision'::text,
        'card_question'::text,
        'card_reasoning'::text,
        'query_grid'::text,
        'conversation_turn'::text,
        'close_line'::text
    ]));

COMMIT;
