"""Synthetic data generator for the insurance agent commissions domain.

Simulates what a real insurance company's source systems would hand off to a
data platform: an agency-management system (agents/agencies), a policy admin
system (products/policies), a commission-calculation engine (commission
plans/transactions/chargebacks), and a finance payment system (payments).

Deliberately injects realistic messiness — duplicate rows, nulls, mixed date
formats, a few fat-finger values, and payment/commission reconciliation
breaks — so the Silver cleaning layer and Gold reconciliation report in this
project have real problems to solve, not a toy dataset.

Usage:
    python -m commissions_pipeline.ingestion.generate_synthetic_data \
        --num-agents 300 --num-policies 15000 --years 2 --seed 42 \
        --output-dir data/raw
"""

from __future__ import annotations

import argparse
import random
import string
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

from commissions_pipeline.config import (
    CHARGEBACK_WINDOW_DAYS,
    OVERRIDE_COMMISSION_RATE,
    PAYMENT_HOLD_THRESHOLD,
)

US_STATES = [
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
]
REGIONS = ["Northeast", "Southeast", "Midwest", "Southwest", "West"]

PRODUCTS = [
    # product_code, product_name, line_of_business, base_commission_rate, premium_low, premium_high
    ("AUTO", "Personal Auto", "Property & Casualty", 0.12, 800, 2600),
    ("HOME", "Homeowners", "Property & Casualty", 0.15, 900, 3200),
    ("RENT", "Renters", "Property & Casualty", 0.15, 120, 400),
    ("UMB", "Personal Umbrella", "Property & Casualty", 0.10, 200, 600),
    ("LTERM", "Term Life", "Life", 0.55, 300, 2500),
    ("LWHOLE", "Whole Life", "Life", 0.70, 1200, 8000),
    ("HLTH", "Supplemental Health", "Health", 0.20, 600, 3600),
]
TIER_MULTIPLIER = {"Bronze": 0.80, "Silver": 1.00, "Gold": 1.15, "Platinum": 1.30}
AGENT_TIERS = list(TIER_MULTIPLIER.keys())
RENEWAL_RATIO = 0.5

CHARGEBACK_REASONS = [
    "Free-look cancellation",
    "Non-payment of premium",
    "Policy rewritten with different carrier",
    "Underwriting rescission",
    "Customer requested cancellation",
]


@dataclass
class GeneratorConfig:
    num_agents: int = 300
    num_policies: int = 15000
    years: int = 2
    seed: int = 42
    output_dir: str = "data/raw"
    end_date: date = None  # defaults to today

    def __post_init__(self):
        if self.end_date is None:
            self.end_date = date.today()

    @property
    def start_date(self) -> date:
        return self.end_date - timedelta(days=365 * self.years)


def _agent_id(i: int) -> str:
    return f"AGT{i:06d}"


def _agency_id(i: int) -> str:
    return f"AGY{i:04d}"


def _policy_id(i: int) -> str:
    return f"POL{i:07d}"


def _txn_id(i: int) -> str:
    return f"TXN{i:08d}"


def _cb_id(i: int) -> str:
    return f"CBK{i:06d}"


def _pay_id(i: int) -> str:
    return f"PAY{i:07d}"


def _rand_date(rng: random.Random, start: date, end: date) -> date:
    span = (end - start).days
    if span <= 0:
        return start
    return start + timedelta(days=rng.randint(0, span))


def gen_agencies(fake: Faker, rng: random.Random, n: int = 18) -> pd.DataFrame:
    rows = []
    for i in range(1, n + 1):
        rows.append(
            {
                "agency_id": _agency_id(i),
                "agency_name": fake.company()
                + " "
                + rng.choice(["Insurance Group", "Agency", "Insurance Partners"]),
                "agency_type": rng.choice(["Captive", "Independent", "Independent", "Franchise"]),
                "region": rng.choice(REGIONS),
            }
        )
    return pd.DataFrame(rows)


def gen_agents(
    fake: Faker, rng: random.Random, cfg: GeneratorConfig, agencies: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Returns (agents_current_snapshot, agent_tier_history) for SCD2 practice."""
    agents, tier_history = [], []
    agency_ids = agencies["agency_id"].tolist()
    senior_pool: dict[str, list[str]] = {aid: [] for aid in agency_ids}

    for i in range(1, cfg.num_agents + 1):
        agent_id = _agent_id(i)
        agency_id = rng.choice(agency_ids)
        hire_date = _rand_date(
            rng, cfg.start_date - timedelta(days=365 * 3), cfg.end_date - timedelta(days=30)
        )
        is_senior = rng.random() < 0.18
        terminated = rng.random() < 0.08
        term_date = None
        status = "Active"
        if terminated:
            term_date = _rand_date(rng, hire_date + timedelta(days=90), cfg.end_date)
            status = "Terminated"
        elif rng.random() < 0.02:
            status = "Suspended"

        email_domain = rng.choice(["gmail.com", "outlook.com", "agencymail.com"])
        first, last = fake.first_name(), fake.last_name()
        email = f"{first}.{last}@{email_domain}".lower()
        if rng.random() < 0.01:  # messy source data
            email = None

        upline_agent_id = None  # filled in second pass
        agents.append(
            {
                "agent_id": agent_id,
                "first_name": first,
                "last_name": last,
                "email": email,
                "hire_date": hire_date.isoformat(),
                "termination_date": term_date.isoformat() if term_date else None,
                "agent_status": status,
                "license_number": "".join(rng.choices(string.digits, k=8)),
                "license_state": (
                    rng.choice(US_STATES) if rng.random() > 0.02 else rng.choice(US_STATES).lower()
                ),
                "agency_id": agency_id,
                "is_senior": is_senior,
                "upline_agent_id": upline_agent_id,
            }
        )
        if is_senior and status != "Terminated":
            senior_pool[agency_id].append(agent_id)

        # SCD2 tier history: 1-3 tier periods across the agent's tenure
        num_periods = rng.choice([1, 1, 2, 3])
        tenure_end = term_date or cfg.end_date
        period_start = hire_date
        tiers_seq = rng.sample(AGENT_TIERS, k=1)
        for _ in range(1, num_periods):
            nxt = rng.choice([t for t in AGENT_TIERS if t != tiers_seq[-1]])
            tiers_seq.append(nxt)
        cuts = sorted(_rand_date(rng, period_start, tenure_end) for _ in range(num_periods - 1))
        boundaries = [period_start] + cuts + [tenure_end]
        for idx, tier in enumerate(tiers_seq):
            eff_start = boundaries[idx]
            eff_end = boundaries[idx + 1] if idx < len(tiers_seq) - 1 else None
            tier_history.append(
                {
                    "agent_id": agent_id,
                    "tier": tier,
                    "effective_start_date": eff_start.isoformat(),
                    "effective_end_date": eff_end.isoformat() if eff_end else None,
                    "is_current": eff_end is None,
                }
            )

    agents_df = pd.DataFrame(agents)

    # second pass: assign ~65% of non-senior agents an upline senior agent in their agency
    for idx, row in agents_df.iterrows():
        if row["is_senior"]:
            continue
        pool = senior_pool.get(row["agency_id"], [])
        pool = [a for a in pool if a != row["agent_id"]]
        if pool and rng.random() < 0.65:
            agents_df.at[idx, "upline_agent_id"] = rng.choice(pool)

    agents_df = agents_df.drop(columns=["is_senior"])

    # inject a couple of exact-duplicate rows to mimic a re-extract landing twice
    if len(agents_df) > 10:
        dupes = agents_df.sample(n=max(1, len(agents_df) // 150), random_state=cfg.seed)
        agents_df = pd.concat([agents_df, dupes], ignore_index=True)

    return agents_df, pd.DataFrame(tier_history)


def gen_products() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "product_id": code,
                "product_name": name,
                "line_of_business": lob,
                "base_commission_rate": rate,
                "premium_low": lo,
                "premium_high": hi,
            }
            for code, name, lob, rate, lo, hi in PRODUCTS
        ]
    )


def gen_commission_plans(products: pd.DataFrame, cfg: GeneratorConfig) -> pd.DataFrame:
    """Rates are plan-year effective-dated so Silver has to do an as-of join."""
    rows = []
    plan_i = 1
    plan_year_starts = [cfg.start_date + timedelta(days=365 * y) for y in range(cfg.years + 1)]
    for y_idx, plan_start in enumerate(plan_year_starts):
        plan_end = (
            plan_year_starts[y_idx + 1] - timedelta(days=1)
            if y_idx + 1 < len(plan_year_starts)
            else None
        )
        # small annual rate drift so history actually differs year over year
        drift = 1.0 + 0.02 * y_idx
        for _, prod in products.iterrows():
            for tier in AGENT_TIERS:
                for txn_type, ratio in [("New Business", 1.0), ("Renewal", RENEWAL_RATIO)]:
                    rate = round(
                        prod["base_commission_rate"] * TIER_MULTIPLIER[tier] * ratio * drift, 4
                    )
                    rows.append(
                        {
                            "plan_id": f"PLN{plan_i:05d}",
                            "product_id": prod["product_id"],
                            "agent_tier": tier,
                            "transaction_type": txn_type,
                            "commission_rate": rate,
                            "effective_start_date": plan_start.isoformat(),
                            "effective_end_date": plan_end.isoformat() if plan_end else None,
                        }
                    )
                    plan_i += 1
    return pd.DataFrame(rows)


def _tier_as_of(tier_history: pd.DataFrame, agent_id: str, as_of: date) -> str | None:
    rows = tier_history[tier_history["agent_id"] == agent_id]
    for _, r in rows.iterrows():
        start = datetime.fromisoformat(r["effective_start_date"]).date()
        end = (
            datetime.fromisoformat(r["effective_end_date"]).date()
            if r["effective_end_date"]
            else None
        )
        if start <= as_of and (end is None or as_of <= end):
            return r["tier"]
    return None


def gen_policies_and_transactions(
    fake: Faker,
    rng: random.Random,
    cfg: GeneratorConfig,
    agents: pd.DataFrame,
    tier_history: pd.DataFrame,
    plans: pd.DataFrame,
    products: pd.DataFrame,
):
    active_agents = agents[agents["agent_status"] != "Terminated"].drop_duplicates("agent_id")
    agent_upline = dict(zip(agents["agent_id"], agents["upline_agent_id"]))

    policies, transactions, chargebacks = [], [], []
    txn_i, cb_i = 1, 1

    def plan_rate(product_id: str, tier: str, txn_type: str, as_of: date) -> float | None:
        cand = plans[
            (plans["product_id"] == product_id)
            & (plans["agent_tier"] == tier)
            & (plans["transaction_type"] == txn_type)
        ]
        for _, r in cand.iterrows():
            start = datetime.fromisoformat(r["effective_start_date"]).date()
            end = (
                datetime.fromisoformat(r["effective_end_date"]).date()
                if r["effective_end_date"]
                else None
            )
            if start <= as_of and (end is None or as_of <= end):
                return r["commission_rate"]
        return None

    for i in range(1, cfg.num_policies + 1):
        policy_id = _policy_id(i)
        agent_row = active_agents.sample(n=1, random_state=rng.randint(0, 10_000_000)).iloc[0]
        agent_id = agent_row["agent_id"]
        hire_date = datetime.fromisoformat(agent_row["hire_date"]).date()
        term_date = (
            datetime.fromisoformat(agent_row["termination_date"]).date()
            if agent_row["termination_date"]
            else None
        )
        window_start = max(cfg.start_date, hire_date)
        window_end = min(cfg.end_date, term_date) if term_date else cfg.end_date
        if window_start >= window_end:
            window_start, window_end = cfg.start_date, cfg.end_date
        issue_date = _rand_date(rng, window_start, window_end)

        product_id, _, _, base_rate, lo, hi = rng.choice(PRODUCTS)
        annual_premium = round(rng.uniform(lo, hi), 2)
        if rng.random() < 0.005:
            annual_premium = None  # messy source

        roll = rng.random()
        if roll < 0.12:
            status = "Cancelled"
        elif roll < 0.22:
            status = "Lapsed"
        elif roll < 0.55:
            status = "Renewed"
        else:
            status = "Active"

        cancel_date = None
        if status == "Cancelled":
            # weighted early: most cancellations happen soon after issue
            days_to_cancel = int(rng.triangular(1, 400, 20))
            cancel_date = issue_date + timedelta(days=days_to_cancel)
            if cancel_date > cfg.end_date:
                cancel_date = cfg.end_date

        policies.append(
            {
                "policy_id": policy_id,
                "policy_number": f"{product_id}-{policy_id[-7:]}",
                "customer_id": fake.uuid4(),
                "product_id": product_id,
                "agent_id": agent_id,
                "issue_date": issue_date.isoformat(),
                "policy_start_date": issue_date.isoformat(),
                "cancel_date": cancel_date.isoformat() if cancel_date else None,
                "annual_premium": annual_premium,
                "policy_status": status,
            }
        )

        if annual_premium is None:
            continue  # can't compute commission on a null premium — Silver DQ will quarantine the policy row itself

        tier_at_issue = _tier_as_of(tier_history, agent_id, issue_date) or "Bronze"
        rate = plan_rate(product_id, tier_at_issue, "New Business", issue_date) or base_rate
        nb_amount = round(annual_premium * rate, 2)
        if rng.random() < 0.002:
            nb_amount = -abs(nb_amount)  # fat-finger sign error for DQ checks to catch

        transactions.append(
            {
                "transaction_id": _txn_id(txn_i),
                "policy_id": policy_id,
                "agent_id": agent_id,
                "product_id": product_id,
                "transaction_type": "New Business",
                "transaction_date": issue_date.isoformat(),
                "premium_amount": annual_premium,
                "commission_rate": rate,
                "commission_amount": nb_amount,
            }
        )
        txn_i += 1

        upline = agent_upline.get(agent_id)
        if upline:
            ov_amount = round(nb_amount * OVERRIDE_COMMISSION_RATE, 2)
            transactions.append(
                {
                    "transaction_id": _txn_id(txn_i),
                    "policy_id": policy_id,
                    "agent_id": upline,
                    "product_id": product_id,
                    "transaction_type": "Override",
                    "transaction_date": issue_date.isoformat(),
                    "premium_amount": annual_premium,
                    "commission_rate": OVERRIDE_COMMISSION_RATE,
                    "commission_amount": ov_amount,
                }
            )
            txn_i += 1

        if status == "Cancelled" and cancel_date is not None:
            if (cancel_date - issue_date).days <= CHARGEBACK_WINDOW_DAYS:
                chargebacks.append(
                    {
                        "chargeback_id": _cb_id(cb_i),
                        "original_transaction_id": (
                            transactions[-2]["transaction_id"]
                            if upline
                            else transactions[-1]["transaction_id"]
                        ),
                        "policy_id": policy_id,
                        "agent_id": agent_id,
                        "chargeback_date": cancel_date.isoformat(),
                        "chargeback_reason": rng.choice(CHARGEBACK_REASONS),
                        "chargeback_amount": -round(nb_amount, 2),
                    }
                )
                cb_i += 1

        if status == "Renewed":
            num_renewals = rng.choice([1, 1, 2])
            for r_idx in range(1, num_renewals + 1):
                renewal_date = issue_date + timedelta(days=365 * r_idx)
                if renewal_date > cfg.end_date:
                    break
                tier_at_renewal = _tier_as_of(tier_history, agent_id, renewal_date) or tier_at_issue
                r_rate = plan_rate(product_id, tier_at_renewal, "Renewal", renewal_date) or (
                    base_rate * RENEWAL_RATIO
                )
                r_amount = round(annual_premium * r_rate, 2)
                transactions.append(
                    {
                        "transaction_id": _txn_id(txn_i),
                        "policy_id": policy_id,
                        "agent_id": agent_id,
                        "product_id": product_id,
                        "transaction_type": "Renewal",
                        "transaction_date": renewal_date.isoformat(),
                        "premium_amount": annual_premium,
                        "commission_rate": r_rate,
                        "commission_amount": r_amount,
                    }
                )
                txn_i += 1
                if upline:
                    ov_amount = round(r_amount * OVERRIDE_COMMISSION_RATE, 2)
                    transactions.append(
                        {
                            "transaction_id": _txn_id(txn_i),
                            "policy_id": policy_id,
                            "agent_id": upline,
                            "product_id": product_id,
                            "transaction_type": "Override",
                            "transaction_date": renewal_date.isoformat(),
                            "premium_amount": annual_premium,
                            "commission_rate": OVERRIDE_COMMISSION_RATE,
                            "commission_amount": ov_amount,
                        }
                    )
                    txn_i += 1

    policies_df = pd.DataFrame(policies)
    transactions_df = pd.DataFrame(transactions)
    chargebacks_df = pd.DataFrame(chargebacks)

    # a couple of duplicate policy rows landing twice (late-arriving correction pattern)
    if len(policies_df) > 20:
        dupes = policies_df.sample(n=max(1, len(policies_df) // 500), random_state=cfg.seed)
        policies_df = pd.concat([policies_df, dupes], ignore_index=True)

    return policies_df, transactions_df, chargebacks_df


def gen_payments(
    rng: random.Random, cfg: GeneratorConfig, transactions: pd.DataFrame, chargebacks: pd.DataFrame
) -> pd.DataFrame:
    txn = transactions.copy()
    txn["transaction_date"] = pd.to_datetime(txn["transaction_date"])
    txn["pay_period"] = txn["transaction_date"].dt.to_period("M").astype(str)
    gross = (
        txn.groupby(["agent_id", "pay_period"])["commission_amount"]
        .sum()
        .rename("gross_commission")
    )

    cb = chargebacks.copy()
    if len(cb) > 0:
        cb["chargeback_date"] = pd.to_datetime(cb["chargeback_date"])
        cb["pay_period"] = cb["chargeback_date"].dt.to_period("M").astype(str)
        cb_sum = (
            cb.groupby(["agent_id", "pay_period"])["chargeback_amount"]
            .sum()
            .rename("chargeback_deductions")
        )
    else:
        cb_sum = pd.Series(name="chargeback_deductions", dtype=float)

    summary = pd.concat([gross, cb_sum], axis=1).fillna(0.0).reset_index()
    summary["net_payment_amount"] = round(
        summary["gross_commission"] + summary["chargeback_deductions"], 2
    )

    rows = []
    for i, r in enumerate(summary.itertuples(index=False), start=1):
        net = r.net_payment_amount
        status = "Held" if net < PAYMENT_HOLD_THRESHOLD else "Paid"
        pay_date = (pd.Period(r.pay_period).end_time + timedelta(days=5)).date()
        # inject a reconciliation break on ~2% of rows: finance system pays a slightly different amount
        recorded_net = net
        if rng.random() < 0.02:
            recorded_net = round(net + rng.uniform(-150, 150), 2)
        rows.append(
            {
                "payment_id": _pay_id(i),
                "agent_id": r.agent_id,
                "pay_period": r.pay_period,
                "payment_date": pay_date.isoformat(),
                "gross_commission": round(r.gross_commission, 2),
                "chargeback_deductions": round(r.chargeback_deductions, 2),
                "net_payment_amount": recorded_net,
                "payment_status": status,
                "payment_method": rng.choice(["ACH", "ACH", "ACH", "Check"]),
            }
        )
    return pd.DataFrame(rows)


def write_reference_table(df: pd.DataFrame, output_dir: Path, name: str, load_date: date) -> None:
    out = output_dir / "reference" / name / f"full_load_date={load_date.isoformat()}"
    out.mkdir(parents=True, exist_ok=True)
    df.to_csv(out / f"{name}.csv", index=False)


def write_incremental_table(df: pd.DataFrame, output_dir: Path, name: str, date_col: str) -> None:
    """Partitions rows by month of `date_col` to mimic daily/monthly source-system drops."""
    d = df.copy()
    d["_ingest_month"] = pd.to_datetime(d[date_col], errors="coerce").dt.to_period("M").astype(str)
    for month, chunk in d.groupby("_ingest_month"):
        out = output_dir / name / f"ingest_month={month}"
        out.mkdir(parents=True, exist_ok=True)
        chunk.drop(columns=["_ingest_month"]).to_csv(out / f"{name}_{month}.csv", index=False)


def generate(cfg: GeneratorConfig) -> dict[str, pd.DataFrame]:
    fake = Faker()
    Faker.seed(cfg.seed)
    rng = random.Random(cfg.seed)
    np.random.seed(cfg.seed)

    agencies = gen_agencies(fake, rng)
    agents, tier_history = gen_agents(fake, rng, cfg, agencies)
    products = gen_products()
    plans = gen_commission_plans(products, cfg)
    policies, transactions, chargebacks = gen_policies_and_transactions(
        fake, rng, cfg, agents, tier_history, plans, products
    )
    payments = gen_payments(rng, cfg, transactions, chargebacks)

    return {
        "agencies": agencies,
        "agents": agents,
        "agent_tier_history": tier_history,
        "products": products,
        "commission_plans": plans,
        "policies": policies,
        "commission_transactions": transactions,
        "chargebacks": chargebacks,
        "payments": payments,
    }


def write_all(
    tables: dict[str, pd.DataFrame], output_dir: str, load_date: date | None = None
) -> None:
    out = Path(output_dir)
    load_date = load_date or date.today()

    for name in ["agencies", "agents", "agent_tier_history", "products", "commission_plans"]:
        write_reference_table(tables[name], out, name, load_date)

    write_incremental_table(tables["policies"], out, "policies", "issue_date")
    write_incremental_table(
        tables["commission_transactions"], out, "commission_transactions", "transaction_date"
    )
    write_incremental_table(tables["chargebacks"], out, "chargebacks", "chargeback_date")
    write_incremental_table(tables["payments"], out, "payments", "payment_date")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--num-agents", type=int, default=300)
    parser.add_argument("--num-policies", type=int, default=15000)
    parser.add_argument("--years", type=int, default=2)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--output-dir", type=str, default="data/raw")
    args = parser.parse_args()

    cfg = GeneratorConfig(
        num_agents=args.num_agents,
        num_policies=args.num_policies,
        years=args.years,
        seed=args.seed,
        output_dir=args.output_dir,
    )
    tables = generate(cfg)
    write_all(tables, cfg.output_dir)

    for name, df in tables.items():
        print(f"{name:>24s}: {len(df):>8,} rows")


if __name__ == "__main__":
    main()
