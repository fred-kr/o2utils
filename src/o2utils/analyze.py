from pathlib import Path
from typing import NamedTuple, TypedDict, cast

import polars as pl
import polars.selectors as cs
from safe_result import Err, Ok, safe
from scipy import stats

from o2utils.common import MetadataRow


class LinregressResult(NamedTuple):
    slope: float
    intercept: float
    rvalue: float
    pvalue: float
    stderr: float
    intercept_stderr: float


class FitResult(TypedDict):
    source_file_cleaned: str
    slope: float | None
    r2: float | None
    mean_temperature: float | None
    start_time: str | None
    stop_time: str | None
    duration: str | None


@safe
def _mean_value(s: pl.Series) -> float:
    return s.mean()


def linear_fit(
    df: pl.DataFrame,
    metadata: MetadataRow,
    x_col: str = "time_seconds",
    y_col: str = "oxygen",
    y2_col: str = "temperature",
) -> tuple[pl.DataFrame, pl.DataFrame]:
    name = df.item(0, "source_file_cleaned")
    res = FitResult(
        source_file_cleaned=name,
        slope=None,
        r2=None,
        mean_temperature=None,
        start_time=None,
        stop_time=None,
        duration=None,
    )
    schema = {
        "source_file_cleaned": pl.Utf8,
        "slope": pl.Float64,
        "r2": pl.Float64,
        "mean_temperature": pl.Float64,
        "start_time": pl.Utf8,
        "stop_time": pl.Utf8,
        "duration": pl.Utf8,
    }

    start = metadata["analysis_start_seconds"]
    stop = metadata["analysis_stop_seconds"]
    if stop == -1:
        stop = df.item(-1, x_col)

    df = df.filter(pl.col(x_col).is_between(start, stop))

    if df.is_empty():
        print(f"no data for {name}")
        return pl.DataFrame(res, schema=schema, strict=False), df.with_columns(
            pl.lit(float("nan")).alias("oxygen_fitted")
        )

    result = cast(LinregressResult, stats.linregress(df.get_column(x_col), df.get_column(y_col)))

    df = df.with_columns((result.slope * pl.col(x_col) + result.intercept).alias("oxygen_fitted"))

    t_start = df.item(0, "datetime_local")
    t_stop = df.item(-1, "datetime_local")

    mean_res = _mean_value(df.get_column(y2_col))

    match mean_res:
        case Ok(v):
            mean_res = v
        case Err(e):
            print(e)
            mean_res = float("nan")
        case _:
            mean_res = float("nan")

    res.update(
        slope=result.slope,
        r2=result.rvalue**2,
        mean_temperature=mean_res,
        start_time=t_start,
        stop_time=t_stop,
    )

    res_df = pl.DataFrame(res, schema=schema, strict=False).with_columns(
        pl.col("slope").round(5),
        pl.col("r2").round(3),
        pl.col("mean_temperature").round(1),
        (pl.col("stop_time").str.to_datetime() - pl.col("start_time").str.to_datetime())
        .dt.to_string("polars")
        .alias("duration"),
    )

    return res_df, df


def get_fit(
    source_file_cleaned: Path, info_file: Path, start: int | None = None, stop: int | None = None
) -> tuple[pl.DataFrame, pl.DataFrame]:
    info = cast(
        MetadataRow,
        pl.read_excel(info_file, sheet_name="metadata").row(
            by_predicate=pl.col("source_file_cleaned") == source_file_cleaned.stem, named=True
        ),
    )

    data = pl.read_csv(source_file_cleaned)

    start = start or info["analysis_start_seconds"]
    stop = stop or info["analysis_stop_seconds"]

    return linear_fit(data, info)


def fit_all(
    folder: Path,
    info_file: Path,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    info_df = pl.read_excel(info_file, sheet_name="metadata")

    data = pl.concat(
        pl.read_csv(f).select(~cs.starts_with("logtime")).with_columns(pl.lit(f.stem).alias("source_file_cleaned"))
        for f in folder.glob("*.csv")
    ).partition_by("source_file_cleaned", include_key=True)

    res_dfs: list[pl.DataFrame] = []
    source_dfs: list[pl.DataFrame] = []

    for df in data:
        source_name = df.item(0, "source_file_cleaned")
        info = cast(
            MetadataRow,
            info_df.row(by_predicate=pl.col("source_file_cleaned") == source_name, named=True),
        )
        res_df, source_df = linear_fit(df, info)
        res_dfs.append(res_df)
        source_dfs.append(source_df)

    return pl.concat(res_dfs), pl.concat(source_dfs)


if __name__ == "__main__":
    input_folder = Path("E:/dev-home/_readonly_datastore_/2025_HMSC/source_cleaned")
    info_file = Path("E:/dev-home/_readonly_datastore_/2025_HMSC/metadata.xlsx")
    res_df, source_df = fit_all(input_folder, info_file)
    res_df.write_excel(
        "E:/dev-home/_readonly_datastore_/2025_HMSC/results/linear_fits.xlsx",
        float_precision=5,
        column_formats={
            "slope": "0.00000",
            "r2": "0.000",
            "mean_temperature": "0.0",
            "start_time": "yyyy-MM-dd HH:mm:ss zzzz",
            "stop_time": "yyyy-MM-dd HH:mm:ss zzzz",
            "duration": "HH:mm:ss",
        },
    )
    # res_df.write_csv("E:/dev-home/_readonly_datastore_/2025_HMSC/results/linear_fits.csv")
    source_df.write_csv("E:/dev-home/_readonly_datastore_/2025_HMSC/results/linear_fits_data.csv")
