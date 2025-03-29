import datetime
import re
import unicodedata
from collections.abc import Sequence
from pathlib import Path

import polars as pl
import polars.selectors as cs


def _normalize_1(name: str) -> str:
    FIXES = [(r"[ /:,?()\.-]", "_"), (r"['’]", ""), (r"[\xa0]", "_")]
    for search, replace in FIXES:
        name = re.sub(pattern=search, repl=replace, string=name)
    return name


def _remove_special(name: Sequence[str]) -> str:
    return "".join(item for item in name if item.isalnum() or (item == "_"))


def _strip_accents(name: str) -> str:
    return "".join(letter for letter in unicodedata.normalize("NFD", name) if not unicodedata.combining(letter))


def _strip_underscores(name: str) -> str:
    return name.strip("_")


def clean_name(
    name: str, strip_accents: bool = True, strip_underscores: bool = True, remove_special: bool = True
) -> str:
    name = name.lower()
    name = _normalize_1(name)
    if strip_accents:
        name = _strip_accents(name)
    if strip_underscores:
        name = _strip_underscores(name)
    if remove_special:
        name = _remove_special(name)
    return name


def combine_to_datetime(
    date_column: str,
    time_column: str,
    separator: str = " ",
    format: str = "%d/%m/%Y %H:%M:%S",
    tz: str | None = None,
    convert_to_tz: str | None = None,
    as_str: bool = True,
) -> pl.Expr:
    out = pl.concat_str([pl.col(date_column, time_column)], separator=separator).str.to_datetime(format, time_zone=tz)
    if convert_to_tz is not None:
        out = out.dt.convert_time_zone(convert_to_tz)
    if as_str:
        out = out.dt.strftime("%Y-%m-%d %H:%M:%S%z")
    return out


def parse_presens_file(
    source: str,
    separator: str = ";",
    skip_rows: int = 57,
    tz_presens: str = "Europe/Berlin",
    tz_local: str = "America/Los_Angeles",
) -> pl.DataFrame:
    file_path = Path(source)

    df = (
        pl.scan_csv(
            file_path,
            separator=separator,
            skip_rows=skip_rows,
        )
        .rename(lambda col: clean_name(col))
        .select(cs.by_dtype(pl.Utf8).str.strip_chars())
        .collect()
    )

    return (
        df.lazy()
        .select(
            pl.lit(file_path.stem).alias("source_file"),
            (
                combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", as_str=False)
                - combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", as_str=False).first()
            )
            .dt.total_seconds()
            .cast(pl.Int32)
            .alias("time_seconds"),
            cs.starts_with("logtime").cast(pl.Float64),
            cs.by_name("oxygen_airsatur", "temp_c", "phase").cast(pl.Float64),
            pl.col("amp").cast(pl.Int32),
            combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", tz=tz_presens).alias("datetime_presens"),
            combine_to_datetime("date_dd_mm_yy", "time_hh_mm_ss", tz=tz_presens, convert_to_tz=tz_local).alias(
                "datetime_local"
            ),
        )
        .rename(
            {
                "oxygen_airsatur": "oxygen",
                "temp_c": "temperature",
                "phase": "phase",
                "amp": "amplitude",
            }
        )
        .collect()
    )


def presens_to_csv(
    presens_folder: str,
    output_folder: str,
    name_mapping: dict[str, str],
    separator: str = ";",
    skip_rows: int = 57,
    tz_presens: str = "Europe/Berlin",
    tz_local: str = "America/Los_Angeles",
) -> None:
    renamed = {f.resolve().as_posix(): name_mapping[f.stem] for f in Path(presens_folder).glob("*.txt")}
    renamed_with_dtm: dict[str, tuple[str, pl.DataFrame]] = {}

    for old, new in renamed.items():
        df = parse_presens_file(old, separator=separator, skip_rows=skip_rows, tz_presens=tz_presens, tz_local=tz_local)
        first_dtm = datetime.datetime.strptime(df.item(0, "datetime_local"), "%Y-%m-%d %H:%M:%S%z")
        new_with_dtm = f"{first_dtm.strftime('%Y%m%dT%H%M%S')}_{new.split('_', 1)[1]}"
        renamed_with_dtm[old] = new_with_dtm, df

    for new, df in renamed_with_dtm.values():
        out_path = (Path(output_folder) / new).with_suffix(".csv")
        df.write_csv(out_path, datetime_format="%Y-%m-%d %H:%M:%S%z")
