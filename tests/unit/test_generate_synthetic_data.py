"""Pure-pandas tests for the synthetic data generator's business rules.

No Spark required — these validate the data itself, independent of the
pipeline that later processes it.
"""

from datetime import date

from commissions_pipeline.config import CHARGEBACK_WINDOW_DAYS
from commissions_pipeline.ingestion.generate_synthetic_data import GeneratorConfig, generate


def _small_dataset():
    cfg = GeneratorConfig(
        num_agents=40, num_policies=500, years=2, seed=7, end_date=date(2026, 1, 1)
    )
    return generate(cfg)


def test_every_chargeback_references_a_real_transaction():
    tables = _small_dataset()
    txn_ids = set(tables["commission_transactions"]["transaction_id"])
    cb_refs = set(tables["chargebacks"]["original_transaction_id"])
    assert cb_refs.issubset(txn_ids)


def test_chargebacks_only_occur_within_the_configured_window():
    tables = _small_dataset()
    policies = tables["policies"].set_index("policy_id")
    cb = tables["chargebacks"]

    for _, row in cb.iterrows():
        policy = policies.loc[row["policy_id"]]
        issue = date.fromisoformat(policy["issue_date"])
        cancel = date.fromisoformat(row["chargeback_date"])
        assert (cancel - issue).days <= CHARGEBACK_WINDOW_DAYS


def test_chargeback_amount_is_negative():
    tables = _small_dataset()
    assert (tables["chargebacks"]["chargeback_amount"] < 0).all()


def test_override_transactions_only_exist_for_agents_with_an_upline():
    tables = _small_dataset()
    agents = tables["agents"].drop_duplicates("agent_id").set_index("agent_id")
    overrides = tables["commission_transactions"][
        tables["commission_transactions"]["transaction_type"] == "Override"
    ]
    # every override recipient must be someone's upline — i.e. appear as an upline_agent_id
    uplines = set(agents["upline_agent_id"].dropna())
    assert set(overrides["agent_id"]).issubset(uplines)


def test_new_business_and_renewal_commission_amounts_are_positive_except_injected_noise():
    tables = _small_dataset()
    txn = tables["commission_transactions"]
    nb_and_renewal = txn[txn["transaction_type"].isin(["New Business", "Renewal"])]
    # a tiny fraction of rows are deliberately fat-fingered negative to exercise DQ checks downstream
    negative_ratio = (nb_and_renewal["commission_amount"] < 0).mean()
    assert negative_ratio < 0.01
