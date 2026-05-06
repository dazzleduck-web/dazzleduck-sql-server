-- Creates and seeds the named_queries database.
-- Run against the target file:
--   duckdb /data/named_queries.duckdb < create_named_queries.sql
--
-- The SQL templates inside each named query reference tables via their
-- attached database prefixes (siem.* and marketing.*), so the server
-- must have both databases attached before executing these queries.
-- query_group column for organizing queries by category
-- ============================================================

CREATE TABLE IF NOT EXISTS named_queries (
    id                     BIGINT PRIMARY KEY,
    name                   VARCHAR UNIQUE,
    template               VARCHAR,
    validators             VARCHAR[],
    description            VARCHAR,
    parameter_descriptions MAP(VARCHAR, VARCHAR),
    preferred_display      VARCHAR,
    query_group             VARCHAR DEFAULT 'general'
);

DELETE FROM named_queries WHERE name IN (
    'marketing_campaign_performance',
    'marketing_conversion_funnel',
    'marketing_leads_by_campaign',
    'marketing_channel_roi',
    'marketing_recent_conversions',
    'siem_alert_summary',
    'siem_top_attacked_hosts',
    'siem_user_activity',
    'siem_mitre_heatmap',
    'siem_recent_critical_events'
);

-- ============================================================
-- Marketing named queries
-- ============================================================

-- 1. Campaign performance
INSERT INTO named_queries VALUES (
    1,
    'marketing_campaign_performance',
    'SELECT
    c.id,
    c.name,
    c.channel,
    c.status,
    c.budget,
    c.spent,
    round(c.spent / nullif(c.budget, 0) * 100, 1)  AS budget_utilisation_pct,
    count(DISTINCT l.id)                            AS leads,
    count(DISTINCT cv.id)                           AS conversions
FROM marketing.campaigns c
LEFT JOIN marketing.leads l        ON l.source_campaign = c.id
LEFT JOIN marketing.conversions cv ON cv.campaign_id    = c.id
{% if channel %}
WHERE c.channel = ''{{ channel }}''
{% endif %}
GROUP BY c.id, c.name, c.channel, c.status, c.budget, c.spent
ORDER BY c.budget DESC
LIMIT {{ limit | default(''20'') }}',
    NULL,
    'Budget utilisation, lead count, and conversion count per campaign. Optionally filter by channel.',
    MAP {
        'channel': 'Optional: filter by channel (email | social | search | display | referral). Leave blank for all.',
        'limit':   'Maximum number of campaigns to return (default 20)'
    },
        'table',
        'marketing');

-- 2. Conversion funnel
INSERT INTO named_queries VALUES (
    2,
    'marketing_conversion_funnel',
    'SELECT
    c.name                                          AS campaign,
    c.channel,
    count(DISTINCT CASE WHEN i.event_type = ''impression'' THEN i.id END) AS impressions,
    count(DISTINCT CASE WHEN i.event_type = ''click''      THEN i.id END) AS clicks,
    count(DISTINCT cv.id)                                                  AS conversions,
    round(
        count(DISTINCT CASE WHEN i.event_type = ''click'' THEN i.id END) * 100.0
        / nullif(count(DISTINCT CASE WHEN i.event_type = ''impression'' THEN i.id END), 0),
    2) AS click_through_rate_pct,
    round(
        count(DISTINCT cv.id) * 100.0
        / nullif(count(DISTINCT CASE WHEN i.event_type = ''click'' THEN i.id END), 0),
    2) AS conversion_rate_pct
FROM marketing.campaigns c
LEFT JOIN marketing.impressions i  ON i.campaign_id = c.id
LEFT JOIN marketing.conversions cv ON cv.campaign_id = c.id
GROUP BY c.name, c.channel
ORDER BY impressions DESC
LIMIT {{ limit | default(''20'') }}',
    NULL,
    'Full conversion funnel: impressions, clicks, conversions, CTR, and conversion rate per campaign.',
    MAP {
        'limit': 'Maximum number of campaigns to return (default 20)'
    },
        'line',
        'marketing');

-- 3. Leads by campaign
INSERT INTO named_queries VALUES (
    3,
    'marketing_leads_by_campaign',
    'SELECT
    l.id,
    l.first_name,
    l.last_name,
    l.email,
    l.company,
    l.created_at,
    CASE WHEN cv.id IS NOT NULL THEN ''yes'' ELSE ''no'' END AS converted
FROM marketing.leads l
JOIN marketing.campaigns c        ON c.id = l.source_campaign
LEFT JOIN marketing.conversions cv ON cv.lead_id = l.id
WHERE c.name = ''{{ campaign_name }}''
ORDER BY l.created_at DESC',
    NULL,
    'Lists all leads attributed to a named campaign, with a flag indicating if they converted.',
    MAP {
        'campaign_name': 'Exact campaign name to look up'
    },
        'pie',
        'marketing');

-- 4. Channel ROI
INSERT INTO named_queries VALUES (
    4,
    'marketing_channel_roi',
    'SELECT
    c.channel,
    count(DISTINCT c.id)             AS campaigns,
    sum(c.budget)                    AS total_budget,
    sum(c.spent)                     AS total_spent,
    coalesce(sum(cv.value), 0)       AS total_revenue,
    round(
        (coalesce(sum(cv.value), 0) - sum(c.spent))
        / nullif(sum(c.spent), 0) * 100,
    2)                               AS roi_pct
FROM marketing.campaigns c
LEFT JOIN marketing.conversions cv ON cv.campaign_id = c.id
{% if status %}
WHERE c.status = ''{{ status }}''
{% endif %}
GROUP BY c.channel
ORDER BY roi_pct DESC',
    NULL,
    'Revenue, spend, and ROI grouped by channel. Optionally filter to a single campaign status.',
    MAP {
        'status': 'Optional: filter by campaign status (active | paused | completed). Leave blank for all.'
    },
        'table',
        'marketing');

-- 5. Recent conversions
INSERT INTO named_queries VALUES (
    5,
    'marketing_recent_conversions',
    'SELECT
    cv.converted_at,
    cv.type                          AS conversion_type,
    cv.value,
    l.first_name || '' '' || l.last_name AS lead_name,
    l.email,
    l.company,
    c.name                           AS campaign,
    c.channel
FROM marketing.conversions cv
JOIN marketing.leads     l ON l.id = cv.lead_id
JOIN marketing.campaigns c ON c.id = cv.campaign_id
{% if type %}
WHERE cv.type = ''{{ type }}''
{% endif %}
ORDER BY cv.converted_at DESC
LIMIT {{ limit | default(''50'') }}',
    NULL,
    'Most recent conversions with lead and campaign details. Optionally filter by conversion type.',
    MAP {
        'type':  'Optional: filter by conversion type (signup | purchase | trial). Leave blank for all.',
        'limit': 'Maximum number of rows to return (default 50)'
    },
        'bar',
        'marketing');

-- ============================================================
-- SIEM named queries
-- ============================================================

-- 6. Alert summary
INSERT INTO named_queries VALUES (
    6,
    'siem_alert_summary',
    'SELECT
    a.severity,
    a.status,
    a.mitre_tactic,
    count(*)                        AS alert_count,
    count(DISTINCT a.host_id)       AS affected_hosts,
    min(a.created_at)               AS first_seen,
    max(a.created_at)               AS last_seen
FROM siem.alerts a
{% if status %}
WHERE a.status = ''{{ status }}''
{% endif %}
GROUP BY a.severity, a.status, a.mitre_tactic
ORDER BY
    CASE a.severity
        WHEN ''critical'' THEN 1
        WHEN ''high''     THEN 2
        WHEN ''medium''   THEN 3
        ELSE 4
    END,
    alert_count DESC',
    NULL,
    'Alert counts grouped by severity, status, and MITRE tactic. Optionally filter by status.',
    MAP {
        'status': 'Optional: filter by alert status (open | investigating | closed | false_positive). Leave blank for all.'
    },
        'table',
        'siem');

-- 7. Top attacked hosts
INSERT INTO named_queries VALUES (
    7,
    'siem_top_attacked_hosts',
    'SELECT
    h.hostname,
    h.ip_address,
    h.environment,
    h.os,
    count(DISTINCT e.id)                                                    AS total_events,
    count(DISTINCT CASE WHEN e.severity IN (''high'',''critical'') THEN e.id END) AS high_sev_events,
    count(DISTINCT a.id)                                                    AS total_alerts,
    count(DISTINCT CASE WHEN a.status = ''open'' THEN a.id END)             AS open_alerts
FROM siem.hosts h
LEFT JOIN siem.events e ON e.host_id = h.id
LEFT JOIN siem.alerts a ON a.host_id = h.id
{% if environment %}
WHERE h.environment = ''{{ environment }}''
{% endif %}
GROUP BY h.hostname, h.ip_address, h.environment, h.os
ORDER BY total_alerts DESC, high_sev_events DESC
LIMIT {{ limit | default(''20'') }}',
    NULL,
    'Hosts ranked by alert and high-severity event count. Optionally filter by environment.',
    MAP {
        'environment': 'Optional: filter by environment (prod | dev | staging). Leave blank for all.',
        'limit':       'Maximum number of hosts to return (default 20)'
    },
        'bar',
        'siem');

-- 8. User activity
INSERT INTO named_queries VALUES (
    8,
    'siem_user_activity',
    'SELECT
    e.occurred_at,
    e.event_type,
    e.severity,
    h.hostname,
    h.ip_address,
    e.source_ip,
    e.dest_ip,
    e.raw_message,
    CASE WHEN a.id IS NOT NULL THEN a.rule_name ELSE NULL END AS triggered_alert
FROM siem.events e
JOIN siem.users u  ON u.id = e.user_id
JOIN siem.hosts h  ON h.id = e.host_id
LEFT JOIN siem.alerts a ON a.event_id = e.id
WHERE u.username = ''{{ username }}''
{% if event_type %}
AND e.event_type = ''{{ event_type }}''
{% endif %}
ORDER BY e.occurred_at DESC
LIMIT {{ limit | default(''100'') }}',
    NULL,
    'All security events for a given username, with host context and any triggered alerts.',
    MAP {
        'username':   'Username to look up (exact match)',
        'event_type': 'Optional: filter by event type (login | logout | file_access | network_connection | privilege_escalation | process_spawn | dns_query). Leave blank for all.',
        'limit':      'Maximum number of events to return (default 100)'
    },
        'line',
        'siem');

-- 9. MITRE heatmap
INSERT INTO named_queries VALUES (
    9,
    'siem_mitre_heatmap',
    'SELECT
    a.mitre_tactic,
    a.mitre_technique,
    count(*)                                                             AS alert_count,
    count(DISTINCT a.host_id)                                           AS affected_hosts,
    count(DISTINCT CASE WHEN a.severity = ''critical'' THEN a.id END)   AS critical_count,
    count(DISTINCT CASE WHEN a.severity = ''high''     THEN a.id END)   AS high_count,
    count(DISTINCT CASE WHEN a.status   = ''open''     THEN a.id END)   AS open_count
FROM siem.alerts a
GROUP BY a.mitre_tactic, a.mitre_technique
HAVING count(*) >= {{ min_count | default(''1'') }}
ORDER BY alert_count DESC',
    NULL,
    'Alert frequency heatmap across MITRE ATT&CK tactics and techniques.',
    MAP {
        'min_count': 'Minimum alert count to include a tactic/technique combination (default 1)'
    },
        'table',
        'siem');

-- 10. Recent critical events
INSERT INTO named_queries VALUES (
    10,
    'siem_recent_critical_events',
    'SELECT
    e.occurred_at,
    e.severity,
    e.event_type,
    h.hostname,
    h.environment,
    u.username,
    u.privilege_level,
    e.source_ip,
    e.dest_ip,
    e.raw_message,
    a.rule_name         AS alert_rule,
    a.mitre_tactic,
    a.status            AS alert_status
FROM siem.events e
JOIN siem.hosts h           ON h.id = e.host_id
LEFT JOIN siem.users u      ON u.id = e.user_id
LEFT JOIN siem.alerts a     ON a.event_id = e.id
WHERE e.severity IN (''{{ severity }}'')
{% if environment %}
AND h.environment = ''{{ environment }}''
{% endif %}
ORDER BY e.occurred_at DESC
LIMIT {{ limit | default(''50'') }}',
    NULL,
    'Most recent events at or above the specified severity, with host, user, and alert context.',
    MAP {
        'severity':    'Severity level to filter on (info | low | medium | high | critical)',
        'environment': 'Optional: restrict to a specific environment (prod | dev | staging). Leave blank for all.',
        'limit':       'Maximum number of events to return (default 50)'
    },
        'pie',
        'siem');

SELECT count(*) AS named_queries_inserted FROM named_queries;
