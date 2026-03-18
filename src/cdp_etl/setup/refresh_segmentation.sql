-- Refresh segmentation materialized views after identity resolution.
-- Must run on a DBSQL Serverless or Pro warehouse (not general compute).

REFRESH MATERIALIZED VIEW ahs_demos_catalog.cdp_360.gold_engagement_metrics;

REFRESH MATERIALIZED VIEW ahs_demos_catalog.cdp_360.gold_segments;
