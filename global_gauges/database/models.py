import logging
import math
from datetime import datetime
from typing import Optional, Any
from enum import Enum

from shapely.geometry import Point
from pydantic import BaseModel, Field, field_validator, model_validator, ConfigDict

logger = logging.getLogger(__name__)

class QualityFlag(str, Enum):
    """
    Enumeration of possible data quality flags.

    Provider class implementations need to decide and define how each provider's
    specific quality notation maps on to these options.
    """

    GOOD = "good"
    PROVISIONAL = "provisional"
    ESTIMATED = "estimated"
    SUSPECT = "suspect"
    BAD = "bad"
    UNKNOWN = "unknown"


class SiteMetadata(BaseModel):
    """
    Pydantic model representing site metadata in the database.

    This model ensures type safety and validation for all site metadata operations.
    All database interactions should use this model for consistency.
    """

    model_config = ConfigDict(
        extra="ignore",
        use_enum_values=True,
        validate_assignment=True,
    )

    site_id: str = Field(..., description="Unique site identifier with provider prefix")
    name: str = Field(..., description="Human-readable site/station name")
    area: Optional[float] = Field(None, description="Drainage area in km²", ge=0)
    active: Optional[bool] = Field(False, description="Whether the site is currently active")
    latitude: float = Field(..., description="Latitude in WGS84", ge=-90, le=90)
    longitude: float = Field(..., description="Longitude in WGS84", ge=-180, le=180)

    # Timestamps and data range information
    last_updated: Optional[datetime] = Field(None, description="When data was last fetched")
    min_date: Optional[datetime] = Field(None, description="Earliest data date available")
    max_date: Optional[datetime] = Field(None, description="Latest data date available")

    # Discharge statistics
    min_discharge: Optional[float] = Field(None, description="Minimum discharge (m³/s)")
    max_discharge: Optional[float] = Field(None, description="Maximum discharge (m³/s)")
    mean_discharge: Optional[float] = Field(None, description="Mean discharge (m³/s)")
    count_discharge: Optional[int] = Field(None, description="Number of discharge records", ge=0)

    provider_misc: Optional[dict[str, Any]] = Field(
        default=None, description="Provider-specific metadata as a JSON-compatible dict"
    )

    @field_validator("area", mode="before")
    @classmethod
    def sanitize_area(cls, v, info):
        # Handle NaN
        if isinstance(v, float) and math.isnan(v):
            return None

        # Handle negative numbers
        if isinstance(v, (int, float)) and v <= 0:
            # Log the bad data for monitoring purposes
            logger.warning(
                f"Negative or zero area'{v}' encountered for site_id '{info.data.get('site_id', 'N/A')}'."
            )
            return None
        return v
    
    @model_validator(mode="after")
    def validate_coordinates(self):
        if self.latitude == 0 and self.longitude == 0:
            logger.warning(
                f"Invalid geometry (0,0) for site '{self.site_id}'. "
                "This `null island` location usually indicates missing coordinate data."
            )
            raise ValueError("Invalid coordinates: latitude and longitude cannot both be zero.")
        return self

    def get_geometry(self) -> Point:
        """Create a Shapely Point geometry from coordinates."""
        return Point(self.longitude, self.latitude)