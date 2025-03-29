import polars as pl


def set_df_format(
    ascii_tables: bool = True, tbl_cols: int = 100, tbl_width_chars: int = 9999, tbl_rows: int = 50
) -> None:
    pl.Config.set_ascii_tables(ascii_tables)
    pl.Config.set_tbl_cols(tbl_cols)
    pl.Config.set_tbl_width_chars(tbl_width_chars)
    pl.Config.set_tbl_rows(tbl_rows)
