-- Gold: Next Best Action (NBA) Engine
-- Assigns a prioritized, personalized action to every merchant based on
-- health score, segment, engagement signals, and support history.
-- Each merchant gets exactly ONE primary action and ONE secondary action.

CREATE OR REPLACE MATERIALIZED VIEW gold_next_best_actions
SCHEDULE REFRESH EVERY 1 HOUR
AS
WITH merchant_profile AS (
  SELECT
    h.golden_id,
    h.segment,
    h.health_score,
    h.health_tier,
    h.r_score,
    h.f_score,
    h.m_score,
    h.txn_count,
    h.txn_volume,
    h.days_since_last_txn,
    h.ticket_count,
    h.tenure_days,
    c.email,
    COALESCE(c.merchant_name, c.first_name, '') AS merchant_name
  FROM gold_health_score h
  JOIN ${catalog}.${schema}.gold_customer_360 c ON h.golden_id = c.golden_id
),
action_rules AS (
  SELECT
    p.*,

    -- Primary action based on segment + health + signals
    CASE
      -- CRITICAL: High-value merchants going dark → executive intervention
      WHEN p.segment IN ('cant_lose') AND p.health_tier IN ('critical', 'poor')
        THEN 'executive_outreach'

      -- CRITICAL: High-value at-risk → personalized win-back
      WHEN p.segment = 'at_risk' AND p.txn_volume > 50000
        THEN 'premium_win_back'

      -- AT_RISK: Standard win-back with incentive
      WHEN p.segment = 'at_risk'
        THEN 'win_back_campaign'

      -- HIBERNATING: Re-engagement or graceful sunset
      WHEN p.segment = 'hibernating' AND p.txn_volume > 10000
        THEN 'reengagement_offer'
      WHEN p.segment = 'hibernating'
        THEN 'sunset_survey'

      -- NEED_ATTENTION: Proactive care
      WHEN p.segment = 'need_attention' AND p.ticket_count > 5
        THEN 'support_escalation'
      WHEN p.segment = 'need_attention'
        THEN 'proactive_checkin'

      -- CHAMPIONS: Maximize value
      WHEN p.segment = 'champions' AND p.tenure_days > 365
        THEN 'loyalty_reward'
      WHEN p.segment = 'champions'
        THEN 'referral_program'

      -- LOYAL: Deepen relationship
      WHEN p.segment = 'loyal' AND p.m_score <= 3
        THEN 'upsell_premium_plan'
      WHEN p.segment = 'loyal'
        THEN 'cross_sell_products'

      -- POTENTIAL_LOYALISTS: Nurture toward loyalty
      WHEN p.segment = 'potential_loyalists'
        THEN 'onboarding_nurture'

      -- NEW_CUSTOMERS: Activate and educate
      WHEN p.segment = 'new_customers' AND p.txn_count <= 3
        THEN 'activation_incentive'
      WHEN p.segment = 'new_customers'
        THEN 'product_education'

      -- PROMISING: Accelerate growth
      WHEN p.segment = 'promising'
        THEN 'growth_acceleration'

      ELSE 'periodic_newsletter'
    END AS primary_action,

    -- Secondary action (always different from primary)
    CASE
      WHEN p.segment IN ('cant_lose') THEN 'custom_pricing_review'
      WHEN p.segment = 'at_risk' AND p.ticket_count > 3 THEN 'satisfaction_survey'
      WHEN p.segment = 'at_risk' THEN 'product_education'
      WHEN p.segment = 'hibernating' THEN 'terminal_upgrade_offer'
      WHEN p.segment = 'need_attention' THEN 'feedback_survey'
      WHEN p.segment = 'champions' THEN 'early_access_features'
      WHEN p.segment = 'loyal' THEN 'referral_program'
      WHEN p.segment = 'potential_loyalists' THEN 'milestone_reward'
      WHEN p.segment = 'new_customers' THEN 'welcome_call'
      WHEN p.segment = 'promising' THEN 'webinar_invitation'
      ELSE 'quarterly_review'
    END AS secondary_action

  FROM merchant_profile p
),
enriched AS (
  SELECT
    a.*,

    -- Action details: channel
    CASE a.primary_action
      WHEN 'executive_outreach' THEN 'phone_inperson'
      WHEN 'premium_win_back' THEN 'phone_sfmc'
      WHEN 'win_back_campaign' THEN 'sfmc_email'
      WHEN 'reengagement_offer' THEN 'zender_sms'
      WHEN 'sunset_survey' THEN 'sfmc_email'
      WHEN 'support_escalation' THEN 'phone'
      WHEN 'proactive_checkin' THEN 'phone'
      WHEN 'loyalty_reward' THEN 'getnet_app_push'
      WHEN 'referral_program' THEN 'sfmc_email'
      WHEN 'upsell_premium_plan' THEN 'phone_sfmc'
      WHEN 'cross_sell_products' THEN 'sfmc_email'
      WHEN 'onboarding_nurture' THEN 'sfmc_journey'
      WHEN 'activation_incentive' THEN 'zender_sms'
      WHEN 'product_education' THEN 'sfmc_email'
      WHEN 'growth_acceleration' THEN 'phone_sfmc'
      ELSE 'sfmc_email'
    END AS primary_channel,

    -- Urgency tier
    CASE
      WHEN a.health_tier = 'critical' THEN 'immediate'
      WHEN a.health_tier = 'poor' THEN 'this_week'
      WHEN a.health_tier = 'fair' THEN 'this_month'
      ELSE 'next_cycle'
    END AS urgency,

    -- Priority score for ranking (lower = more urgent)
    -- Combines health inversion with value weighting
    ROUND(
      (100 - a.health_score) * 0.6
      + CASE
          WHEN a.txn_volume > 100000 THEN 30
          WHEN a.txn_volume > 50000 THEN 20
          WHEN a.txn_volume > 10000 THEN 10
          ELSE 0
        END
      + CASE
          WHEN a.segment IN ('cant_lose', 'at_risk') THEN 10
          WHEN a.segment = 'hibernating' THEN 5
          ELSE 0
        END
    , 1) AS priority_score,

    -- Estimated revenue impact of action
    CASE
      WHEN a.segment IN ('cant_lose', 'at_risk') THEN ROUND(a.txn_volume * 0.7, 2)
      WHEN a.segment = 'hibernating' THEN ROUND(a.txn_volume * 0.3, 2)
      WHEN a.segment = 'champions' THEN ROUND(a.txn_volume * 0.15, 2)
      WHEN a.segment = 'loyal' THEN ROUND(a.txn_volume * 0.2, 2)
      WHEN a.segment IN ('potential_loyalists', 'promising') THEN ROUND(a.txn_volume * 0.25, 2)
      WHEN a.segment = 'new_customers' THEN ROUND(a.txn_volume * 0.5, 2)
      ELSE ROUND(a.txn_volume * 0.1, 2)
    END AS estimated_revenue_impact

  FROM action_rules a
)
SELECT
  golden_id,
  merchant_name,
  email,
  segment,
  health_score,
  health_tier,
  primary_action,
  secondary_action,
  primary_channel,
  urgency,
  priority_score,
  estimated_revenue_impact,
  txn_volume,
  txn_count,
  days_since_last_txn,
  ticket_count,
  tenure_days,
  r_score,
  f_score,
  m_score,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM enriched;
