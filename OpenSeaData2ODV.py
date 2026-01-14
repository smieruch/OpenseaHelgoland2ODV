#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Converts Helgoland OpenSea data from Felswatt to ODV Generic Spreadsheet

Reads:
  - Helgoland OpenSea data .xlxs

Outputs:
  - ODV Generic Spreadsheet .txt

Authors: Sebastian Mieruch (sebastian.mieruch@awi.de)
"""

from pathlib import Path
import pandas as pd
pd.set_option("display.max_columns", None)
pd.set_option("display.width", None)


###-------------------set parameters-------------------###
DATA_DIR = Path("/home/smieruch/Projects/Helgoland")
HEADER_FILE = DATA_DIR / "helgoland_odv_header.txt"
CRUISE_NAME = "Helgoland_OpenSea"
STATION_NAME = "Felswatt"
TYPE_NAME = "B"
OUTFILE = DATA_DIR / f"{CRUISE_NAME}_{STATION_NAME}.txt"
###----------------------------------------------------###



###------------------create data model------------------###
"""
SPEC is the data model that can be easily extended.
Keep in mind to adapt the header file if SPEC is changed!
3 rules are available:
- "fn" applies a function to the input, then maps the transformed data to output
- "ref" copies a column to new output
- "src" just copies input to output

We will loop over SPEC keys and apply what is defined in the values (rules).
"""
SPEC = {
    "Cruise": {"fn": lambda df, path: CRUISE_NAME},
    "Station": {"fn": lambda df, path: STATION_NAME},
    "Type": {"fn": lambda df, path: TYPE_NAME},
    "yyyy-mm-dd": {"fn": lambda df, path: year_month_to_iso(df)},
    "xlon": {"fn": lambda df, path: decode_groups3_scaled_to_decimal_series(df["Longitude"])},
    "xlat": {"fn": lambda df, path: decode_groups3_scaled_to_decimal_series(df["Latitude"])},
    "time_ISO8601": {"ref": "yyyy-mm-dd"},
    #use later in header https://vocab.nerc.ac.uk/collection/P01/current/TEMPP901/
    "Temperature Ocean [~^o~#C]": {"src": "Temperatur °C (Meer)"},
    "Temperature Air [~^o~#C]": {"src": "Temperatur °C (Luft)"},
    #https://vocab.nerc.ac.uk/collection/P01/current/PHXXZZXX/
    "pH": {"src": "pH-Wert"},
    #https://vocab.nerc.ac.uk/collection/P01/current/PSALZZXX/
    "Practical Salinity": {"src": "Salinität (‰)"},
    "Wind Speed [m/s]": {"src": "Windgeschwindigkeit (m/s)"},
    "Illuminance [lux]": {"src": "Lichtintensität (lux)"},
}


###----------------------------------------------------###


def year_month_to_iso(df: pd.DataFrame) -> pd.Series:
    """
    Build ISO-8601 YYYY-MM strings from Year and Month columns.

    Parameters
    ----------
    df : pandas.DataFrame
        Must contain integer columns 'Year' and 'Month'.

    Returns
    -------
    pandas.Series
        ISO-8601 formatted year-month-day strings, e.g. '2024-03-01'.
    """
    return (
        df["Jahr"].astype(int).astype(str)
        + "-"
        + df["Monat"].astype(int).astype(str).str.zfill(2)
        + "-01"
    )


def decode_groups3_scaled_to_decimal_series(s: pd.Series) -> pd.Series:
    """
    Convert Excel-encoded D,MMM,SSS coordinate to decimal degrees.
    """
    
    x = s.astype("Int64")
    sign = (x < 0).map({True: -1, False: 1})
    x = x.abs()

    # Excel stores D,MMM,SSS where MMM and SSS are thousandths,
    #remove last 6 digits
    deg = x // 1_000_000
    #remove last 3 digits, take the remainder of modulo 1000, i.e. remove the first digit 
    mmm = (x // 1_000) % 1_000 
    # remainder modulo 1000 (modulo=what is left after devision)
    sss = x % 1_000 

    minutes = mmm * 60.0 / 1000.0
    seconds = sss * 60.0 / 1000.0

    return sign * (deg + minutes / 60.0 + seconds / 3600.0)


def transform_one_file(df_in: pd.DataFrame, path: Path) -> pd.DataFrame:
    out = pd.DataFrame(index=df_in.index)

    for out_col, rule in SPEC.items():
        if "fn" in rule:
            out[out_col] = rule["fn"](df_in, path)
        elif "src" in rule:
            src = rule["src"]
            out[out_col] = df_in[src] if src in df_in.columns else pd.NA
        elif "ref" in rule:
            out[out_col] = out[rule["ref"]]
        else:
            out[out_col] = pd.NA
    return out



def write_odv_with_header(df_out, header_path: Path, outfile: Path) -> None:
    header_text = header_path.read_text(encoding="utf-8")

    # 1) write the header (overwrite any existing output)
    outfile.write_text(header_text.rstrip("\n") + "\n", encoding="utf-8")

    # 2) append the data table (no header line from pandas)
    df_out.to_csv(
        outfile,
        sep="\t",
        index=False,
        header=False,
        mode="a",
        encoding="utf-8",
        lineterminator="\n",
    )


def main():
    rows = []
    #glob=find files by pattern
    for path in DATA_DIR.glob("*.xlsx"):
        df_in = pd.read_excel(path) 
        df_in.columns = df_in.columns.str.strip() # remove space
        rows.append(transform_one_file(df_in, path))

    df_out = pd.concat(rows, ignore_index=True)

    print(df_out.head())

    ###print to disc
    write_odv_with_header(df_out, HEADER_FILE, OUTFILE)




if __name__ == "__main__":
    main()
