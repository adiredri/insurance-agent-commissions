"""Central configuration for the commissions pipeline.

All paths default to the local filesystem so the whole project runs with zero
cloud dependencies. Set LAKEHOUSE_ROOT (e.g. to an abfss:// or dbfs:/ path) to
point the same code at Azure Data Lake Storage / Databricks without touching
any transformation logic.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _root() -> str:
    return os.environ.get("LAKEHOUSE_ROOT", "data")


@dataclass(frozen=True)
class LakehousePaths:
    root: str = field(default_factory=_root)

    @property
    def raw(self) -> str:
        """Raw CSV landing zone — simulated source-system extracts, not part of the lakehouse proper."""
        return f"{self.root}/raw"

    @property
    def bronze(self) -> str:
        return f"{self.root}/bronze"

    @property
    def silver(self) -> str:
        return f"{self.root}/silver"

    @property
    def gold(self) -> str:
        return f"{self.root}/gold"

    def bronze_table(self, name: str) -> str:
        return f"{self.bronze}/{name}"

    def silver_table(self, name: str) -> str:
        return f"{self.silver}/{name}"

    def gold_table(self, name: str) -> str:
        return f"{self.gold}/{name}"


PATHS = LakehousePaths()

# Business rules — kept in one place so they're easy to cite in interviews.
CHARGEBACK_WINDOW_DAYS = 90  # commission clawed back if policy cancels within this window
RENEWAL_COMMISSION_RATIO = 0.5  # renewal commissions pay at 50% of first-year rate
OVERRIDE_COMMISSION_RATE = 0.10  # upline agent earns 10% override on downline production
PAYMENT_HOLD_THRESHOLD = -500.0  # negative net commission below this puts a payment "on hold"

AGENT_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
POLICY_STATUSES = ["Active", "Lapsed", "Cancelled", "Renewed"]
TRANSACTION_TYPES = ["New Business", "Renewal", "Override", "Chargeback"]
PAYMENT_STATUSES = ["Paid", "Held", "Scheduled", "Failed"]
