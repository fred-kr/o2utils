from pathlib import Path
from typing import Literal, NamedTuple, TypedDict, cast

import polars as pl
import polars.selectors as cs
from scipy import stats


class LinregressResult(NamedTuple):
    slope: float
    intercept: float
    rvalue: float
    pvalue: float
    stderr: float
    intercept_stderr: float


class MetadataRow(TypedDict):
    source_file: str
    cleaned_source_file: str
    start_time_newport: str
    record_type: str
    female_id: str
    target_temperature: Literal["0C", "4C"]
    atm_pressure_mb: float
    n_eggs: float
    n_eggs_weighed: float
    measured_fresh_weight_grams: float
    adjusted_fresh_weight_grams: float
    bacteria_group: str
    vol_respiration_chamber_ml: float
    analysis_start_seconds: int
    analysis_stop_seconds: int
    record_flag: int
    record_flag_description: str
    comment: str


def linear_fit(
    df: pl.DataFrame,
    metadata: MetadataRow,
    x_col: str = "time_seconds",
    y_col: str = "oxygen",
    y2_col: str = "temperature",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    name = df.item(0, "cleaned_source_file")

    start = metadata["analysis_start_seconds"]
    stop = metadata["analysis_stop_seconds"]

    df = df.filter(pl.col(x_col).is_between(start, stop))

    result = cast(LinregressResult, stats.linregress(df.get_column(x_col), df.get_column(y_col)))

    df = df.with_columns((result.slope * pl.col(x_col) + result.intercept).alias("oxygen_fitted"))

    t_start = df.item(0, "datetime_local")
    t_stop = df.item(-1, "datetime_local")

    res_df = pl.DataFrame(
        {
            "source_file": name,
            "slope": result.slope,
            "r2": result.rvalue**2,
            "mean_temperature": df.get_column(y2_col).mean(),
            "start_time": t_start,
            "stop_time": t_stop,
            "duration": None,
        },
        schema={
            "source_file": pl.Utf8,
            "slope": pl.Float64,
            "r2": pl.Float64,
            "mean_temperature": pl.Float64,
            "start_time": pl.Utf8,
            "stop_time": pl.Utf8,
            "duration": pl.Utf8,
        },
        strict=False,
    ).with_columns(
        pl.col("slope").round(5),
        pl.col("r2").round(3),
        pl.col("mean_temperature").round(1),
        (pl.col("stop_time").str.to_datetime() - pl.col("start_time").str.to_datetime())
        .dt.to_string("polars")
        .alias("duration"),
    )

    return res_df, df


def get_fit(
    source_file: Path, info_file: Path, start: int | None = None, stop: int | None = None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    info = cast(
        MetadataRow,
        pl.read_excel(info_file, sheet_name="metadata").row(
            by_predicate=pl.col("cleaned_source_file") == source_file.stem, named=True
        ),
    )

    data = pl.read_csv(source_file)

    start = start or info["analysis_start_seconds"]
    stop = stop or info["analysis_stop_seconds"]

    return linear_fit(data, info)


def fit_all(
    folder: Path,
    info_file: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    info_df = pl.read_excel(info_file, sheet_name="metadata")

    data = pl.concat(pl.read_csv(f).select(~cs.starts_with("logtime")) for f in folder.glob("*.csv")).partition_by(
        "source_file", include_key=True
    )

    res_dfs: list[pl.DataFrame] = []
    source_dfs: list[pl.DataFrame] = []

    for df in data:
        info = cast(
            MetadataRow,
            info_df.row(by_predicate=pl.col("cleaned_source_file") == df.item(0, "source_file"), named=True),
        )
        res_df, source_df = linear_fit(df, info)
        res_dfs.append(res_df)
        source_dfs.append(source_df)

    return pl.concat(res_dfs), pl.concat(source_dfs)
