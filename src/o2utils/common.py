import datetime
from typing import Literal, TypedDict, cast

import polars as pl

METADATA = (
    pl.read_excel("E:/dev-home/_readonly_datastore_/2025_HMSC/metadata.xlsx", sheet_name="metadata")
    .with_columns(pl.col(pl.Float64).round(4))
    .with_columns(
        pl.col("fertilization_time", "start_time_newport").str.to_datetime(
            "%Y-%m-%d %H:%M:%S", time_zone="America/Los_Angeles", strict=False
        )
    )
)
DATA = pl.read_ipc("E:/dev-home/_readonly_datastore_/2025_HMSC/intermediate_outputs/source_cleaned_combined.arrow")


class MetadataRow(TypedDict):
    days_post_fertilization: int | None
    fertilization_time: datetime.datetime | None
    source_file: str
    source_file_cleaned: str
    start_time_newport: datetime.datetime
    record_type: Literal["eggs", "bacteria"]
    female_id: (
        Literal["F01", "F02", "F03", "F04", "F05", "F06", "F07", "F08", "F09", "F10", "F11", "F12", "F13", "F14"] | None
    )
    target_temperature: Literal["0C", "4C"]
    atm_pressure_mb: float
    n_eggs: float | None
    n_eggs_weighed: float | None
    measured_fresh_weight_grams: float | None
    adjusted_fresh_weight_grams: float | None
    bacteria_group: Literal[
        "0C_01",
        "4C_01",
        "4C_02",
        "4C_03",
        "0C_02",
        "4C_04",
        "4C_05",
        "0C_03",
        "0C_04",
        "0C_05",
        "0C_06",
        "0C_07",
        "0C_08",
        "0C_09",
        "0C_10",
        "0C_11",
        "0C_12",
        "0C_13",
        "0C_14",
        "0C_15",
        "0C_16",
    ]
    vol_respiration_chamber_ml: float | None
    analysis_start_seconds: int
    analysis_stop_seconds: int
    record_flag: int
    record_flag_description: str
    comment: str | None


def get_metadata(source: str) -> MetadataRow:
    return cast(MetadataRow, METADATA.row(by_predicate=pl.col("source_file_cleaned") == source, named=True))
