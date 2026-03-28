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
        self.row_count += len(values)

        for value in values:

            if value is None:
                self.null_count += 1
            else:
                if self.min_value is None or value < self.min_value:
                    self.min_value = value
                
                if self.max_value is None or value > self.max_value:
                    self.max_value = value
        
        if self.min_value == self.max_value:
            self.constant = True
            self.constant_value = self.min_value
        else:
            self.constant = False
            self.constant_value = None
        
            

    def merge(self, other: BaseStatistics) -> None:
        """Merge another statistics object into this one."""
        self.row_count += other.row_count
        self.null_count += other.null_count

        if other.null_count != other.row_count:
            self.min_value = min(self.min_value, other.min_value) if self.min_value else other.min_value
            self.max_value = max(self.max_value, other.max_value) if self.max_value else other.max_value

        if self.min_value == self.max_value:
            self.constant = True
            self.constant_value = self.min_value
        else:
            self.constant = False
            self.constant_value = None


    def is_constant(self) -> bool:
        """Return whether all non-null values are the same."""
        return self.constant

    def serialize(self) -> dict[str, Any]:
        """Serialize the statistics object."""
        return self.__dict__

    @classmethod
    def deserialize(cls, payload: dict[str, Any]) -> BaseStatistics:
        """Deserialize the statistics object."""
        return BaseStatistics(**payload)
