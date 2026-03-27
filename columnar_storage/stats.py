"""Statistics skeleton.

The article highlights how checkpoint code aggregates segment statistics upward
into column and table metadata. This module keeps a compact teaching version.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class BaseStatistics:
    """Metadata summary for one logical value stream.

    Goal for the candidate:
    - update `min_value`, `max_value`, and `null_count`
    - detect constant segments
    - merge child statistics into parent statistics
    - serialize cleanly to dictionaries
    """

    min_value: Any = None
    max_value: Any = None
    null_count: int = 0
    row_count: int = 0
    constant: bool = False
    constant_value: Any = None

    def update(self, values: list[Any]) -> None:
        """Update the statistics from a batch of values."""
        raise NotImplementedError("Question 3: implement BaseStatistics.update()")

    def merge(self, other: "BaseStatistics") -> None:
        """Merge another statistics object into this one."""
        raise NotImplementedError("Question 3: implement BaseStatistics.merge()")

    def is_constant(self) -> bool:
        """Return whether all non-null values are the same."""
        raise NotImplementedError("Question 3: implement BaseStatistics.is_constant()")

    def serialize(self) -> dict[str, Any]:
        """Serialize the statistics object."""
        raise NotImplementedError("Question 3: implement BaseStatistics.serialize()")

    @classmethod
    def deserialize(cls, payload: dict[str, Any]) -> "BaseStatistics":
        """Deserialize the statistics object."""
        raise NotImplementedError("Question 3: implement BaseStatistics.deserialize()")
