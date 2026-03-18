-- Create operational tables required by Gold pipeline materialized views.
-- Must run BEFORE the Gold pipeline since gold_campaign_roi and
-- gold_personalization_signals reference nba_action_log.

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.nba_action_log (
  action_id STRING DEFAULT uuid(),
  golden_id STRING NOT NULL,
  action_type STRING NOT NULL,
  channel STRING,
  executed_by STRING,
  notes STRING,
  executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.agent_feedback_log (
  feedback_id STRING DEFAULT uuid(),
  message_content STRING,
  rating INT,
  comment STRING,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);
