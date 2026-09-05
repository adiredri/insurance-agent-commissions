# Data Dictionary — Gold Layer

These are the tables a BI tool or analyst would actually query. Silver tables share
the same core columns minus the business-logic ones (`net_commission`, `chargeback_rate`,
`reconciliation_status`, etc.), plus quarantine tables of the same name under `silver/_quarantine/`.

## `gold.agent_commission_summary`
One row per agent per pay period (`YYYY-MM`). The commission payroll would run off this table.

| Column | Type | Description |
|---|---|---|
| `agent_id` | string | Business key |
| `agent_name`, `agency_id`, `tier`, `agent_status` | string | Current agent attributes (SCD1 enrichment for readability) |
| `pay_period` | string | `YYYY-MM` |
| `new_business_commission` | double | Sum of New Business transaction commissions |
| `renewal_commission` | double | Sum of Renewal transaction commissions |
| `override_commission` | double | Sum of Override (upline) commissions earned |
| `gross_commission` | double | Sum of the three above |
| `chargeback_deductions` | double | Sum of chargebacks in the period (negative) |
| `net_commission` | double | `gross_commission + chargeback_deductions` — what the agent is owed |

## `gold.payment_run_summary`
One row per pay period per agency. Finance-facing rollup of what was actually disbursed.

| Column | Type | Description |
|---|---|---|
| `pay_period`, `agency_id`, `agency_name`, `region` | string | Grouping keys |
| `num_payments` | long | Count of agent payments in the run |
| `total_gross_commission` | double | Sum of gross commission paid |
| `total_chargeback_deductions` | double | Sum of chargeback deductions applied |
| `total_net_paid` | double | Sum of net payments |
| `num_held`, `num_failed` | long | Payments that didn't go out cleanly this run |

## `gold.chargeback_exposure`
One row per agent. Risk-monitoring table — flags agents with abnormally high cancellation-driven clawbacks.

| Column | Type | Description |
|---|---|---|
| `agent_id`, `agent_name`, `agency_id`, `tier` | string | Agent attributes |
| `new_business_commission` | double | Lifetime new business commission earned |
| `total_chargeback_amount` | double | Lifetime chargebacks (absolute value) |
| `chargeback_rate` | double | `total_chargeback_amount / new_business_commission` |
| `is_high_risk` | boolean | `chargeback_rate > 0.25` |

## `gold.agent_leaderboard`
One row per agent, trailing 12 months, ranked by net commission.

| Column | Type | Description |
|---|---|---|
| `agent_id`, `agent_name`, `agency_id`, `tier` | string | Agent attributes |
| `trailing_12mo_gross_commission` / `net_commission` / `chargebacks` | double | Rolling 12-month totals |
| `rank` | int | `RANK()` over net commission, descending |

## `gold.reconciliation_report`
One row per agent per pay period. Compares the commission engine's *calculated* net commission
against the finance system's *recorded* payment — the control that catches payment discrepancies.

| Column | Type | Description |
|---|---|---|
| `agent_id`, `agent_name`, `pay_period` | string | Grouping keys |
| `calculated_net_commission` | double | From `agent_commission_summary` |
| `recorded_net_payment` | double | From `fact_payments` |
| `payment_status` | string | Status of the recorded payment, if any |
| `variance` | double | `recorded − calculated` |
| `reconciliation_status` | string | `MATCHED`, `BREAK` (variance > $1), `CALCULATED_BUT_NOT_PAID`, or `PAID_WITHOUT_CALCULATION` |

## Data quality log — `silver/_dq_log`
Every DQ check run against every Silver table is appended here (not just failures) — `table_name`,
`check_name`, `severity`, `failing_rows`, `total_rows`, `passed`, `run_timestamp`. Point a dashboard
at this table to watch data quality trend over time.
