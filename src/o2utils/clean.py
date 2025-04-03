import datetime
from pathlib import Path

import polars as pl
import polars.selectors as cs
from janitor.polars import clean_names


def combine_to_datetime(
    date_column: str,
    time_column: str,
    separator: str = " ",
    format: str = "%d/%m/%y %H:%M:%S",
    tz: str | None = None,
    convert_to_tz: str | None = None,
    as_str: bool = True,
) -> pl.Expr:
    """
    Combine two columns containing date and time as strings into a single datetime column.

    Parameters
    ----------
    date_column : str
        Name of the column containing the date as a string.
    time_column : str
        Name of the column containing the time as a string.
    separator : str, optional
        The separator to use when combining the date and time columns, by default " "
    format : str, optional
        The format of the combined date and time string, by default "%d/%m/%y %H:%M:%S"
    tz : str, optional
        The timezone of the recorded date and time (determined by the pc it was recorded on), by default None
    convert_to_tz : str, optional
        A timezone to convert to from the recorded timezone, by default None
    as_str : bool, optional
        If True, return the datetime column as a string in the format "%Y-%m-%d %H:%M:%S%z", by default True
    """
    out = pl.concat_str([pl.col(date_column, time_column)], separator=separator).str.to_datetime(format, time_zone=tz)
    if convert_to_tz is not None:
        out = out.dt.convert_time_zone(convert_to_tz)
    if as_str:
        out = out.dt.strftime("%Y-%m-%d %H:%M:%S%z")
    return out


def parse_presens_file(
    source: str | Path,
    renamed_source: str,
    separator: str = ";",
    skip_rows: int = 57,
    tz_presens: str = "Europe/Berlin",
    tz_local: str = "America/Los_Angeles",
) -> pl.DataFrame:
    """
    Parse a presens file into a polars DataFrame.
    """
    file_path = Path(source)

    df: pl.DataFrame = clean_names(
        pl.scan_csv(
            file_path,
            separator=separator,
            skip_rows=skip_rows,
        )
        .select(cs.by_dtype(pl.Utf8).str.strip_chars())
        .collect(),
        remove_special=True,
        strip_underscores=True,
        strip_accents=True,
    )

    logtime_col = df.get_column("logtime_min", default=None)
    if logtime_col is None:
        logtime_col = df.get_column("logtime_h").cast(pl.Float64) * 60
    else:
        logtime_col = logtime_col.cast(pl.Float64)

    return df.select(
        pl.lit(file_path.stem).alias("source_file"),
        pl.lit(renamed_source).alias("source_file_cleaned"),
        (
            combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", as_str=False)
            - combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", as_str=False).first()
        )
        .dt.total_seconds()
        .cast(pl.Int32)
        .alias("time_seconds"),
        logtime_col.round(3).alias("logtime_min"),
        cs.by_name("oxygen_airsatur", "temp_c", "phase").cast(pl.Float64),
        pl.col("amp").cast(pl.Int32),
        combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", tz=tz_presens).alias("datetime_presens"),
        combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", tz=tz_presens, convert_to_tz=tz_local).alias(
            "datetime_local"
        ),
    ).rename(
        {
            "oxygen_airsatur": "oxygen",
            "temp_c": "temperature",
            "phase": "phase",
            "amp": "amplitude",
        }
    )


def presens_to_csv(
    presens_folder: Path,
    output_folder: Path,
    name_mapping: dict[str, str],
    separator: str = ";",
    skip_rows: int = 57,
    tz_presens: str = "Europe/Berlin",
    tz_local: str = "America/Los_Angeles",
) -> None:
    """
    Convert a folder of presens files into cleaned up csv's and standardize file names
    """
    renamed = {f.resolve().as_posix(): name_mapping[f.stem] for f in Path(presens_folder).glob("*.txt")}
    renamed_with_dtm: dict[str, tuple[str, pl.DataFrame]] = {}

    for old, new in renamed.items():
        df = parse_presens_file(
            old, new, separator=separator, skip_rows=skip_rows, tz_presens=tz_presens, tz_local=tz_local
        )
        first_dtm_string = df.item(0, "datetime_local")
        first_dtm = datetime.datetime.strptime(first_dtm_string, "%Y-%m-%d %H:%M:%S%z")
        new_with_dtm = f"{first_dtm.strftime('%Y%m%dT%H%M%S')}_{new.split('_', 1)[1]}"
        renamed_with_dtm[old] = new_with_dtm, df

    for _, (dtm_name, df) in renamed_with_dtm.items():
        out_path = Path(output_folder / dtm_name).with_suffix(".csv")
        df.write_csv(out_path, datetime_format="%Y-%m-%d %H:%M:%S%z")


if __name__ == "__main__":
    presens_folder = Path("E:/dev-home/_readonly_datastore_/2025_HMSC/source_files")
    output_folder = Path("E:/dev-home/_readonly_datastore_/2025_HMSC/source_cleaned")
    name_mapping = pl.read_excel("E:/dev-home/_readonly_datastore_/2025_HMSC/metadata.xlsx", sheet_name="name_map")
    rename_map = {row[0]: row[1] for row in name_mapping.iter_rows()}
    presens_to_csv(presens_folder, output_folder, rename_map)
