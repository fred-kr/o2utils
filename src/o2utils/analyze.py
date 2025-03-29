from pathlib import Path
from typing import NamedTuple, cast

import polars as pl
from scipy import stats


class LinregressResult(NamedTuple):
    slope: float
    intercept: float
    rvalue: float
    pvalue: float
    stderr: float
    intercept_stderr: float


def linear_fit(
    df: pl.DataFrame,
    start: int,
    stop: int,
    x_col: str = "time_seconds",
    y_col: str = "oxygen",
    y2_col: str = "temperature",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    name = df.item(0, "source_file")
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


def get_fit(source_file: Path, info_file: str) -> tuple[pl.DataFrame, pl.DataFrame]:
    info = pl.read_excel(info_file, sheet_name="metadata").row(
        by_predicate=pl.col("cleaned_source_file") == source_file.stem, named=True
    )

    data = pl.read_csv(source_file)

    return linear_fit(data, start=info["analysis_start"], stop=info["analysis_stop"])
