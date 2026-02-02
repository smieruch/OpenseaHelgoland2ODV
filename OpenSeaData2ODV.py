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
DATA_DIR = Path("/home/smieruch/Projects/Helgoland/Full")
INPUT_XLSX = "Abiotics ODV.xlsx"
SHEET_NAMES = ["Abiotics Sea", "Abiotics Pool", "Abiotics Harbour"]
HEADER_FILE = DATA_DIR / "helgoland_odv_header.txt"
CRUISE_NAME = "Helgoland_OpenSea"
#STATION_NAME = "Felswatt" -> we will use the SHEET_NAMES
TYPE_NAME = "B"
OUTFILE = DATA_DIR / f"{CRUISE_NAME}.txt"
###
###---new meta variables---###
#INSTRUMENT_TEMPERATURE = ""
#INSTRUMENT_SALINITY = ""
#REPORT = "report.pdf"
###----------------------------------------------------###





###-------------helper functions---------------------------------------###
def date_to_iso(df: pd.DataFrame, **_) -> pd.Series:
    ###empty Time values are read as NaN
    ###replace empty as ""
    if "Time" in df.columns:
        xtime = (
            df["Time"]
            .where(df["Time"].isna(), "T" + df["Time"].astype(str))
            .fillna("")
        )
    else:
        xtime = pd.Series("", index=df.index)
    
    return (
        df["Year"].astype(int).astype(str)
        + "-"
        + df["Month"].astype(int).astype(str).str.zfill(2)
        + "-"
        + df["Day"].astype(int).astype(str).str.zfill(2)
        + xtime
    )


def add_station_name(df: pd.DataFrame, sheet_name: str, **_) -> str:
    return sheet_name
###------------------------------------------------------------------###


###------------------create data model------------------###
"""
SPEC is the data model that can be easily extended.
Keep in mind to adapt the header file if SPEC is changed!
3 rules are available:
- "const" just adds a constant to the output
- "fn" applies a function to the input, then maps the transformed data to output
- "ref" copies a column to new output
- "src" just copies input to output

We will loop over SPEC keys and apply what is defined in the values (rules).
"""
SPEC = {
    "Cruise": {"const": CRUISE_NAME},
    "Station": {"fn": add_station_name},
    "Type": {"const": TYPE_NAME},
    "yyyy-mm-ddThh:mm": {"fn": date_to_iso},
    "xlon": {"src": "Longitude [degrees_east]"},
    "xlat": {"src": "Latitude [degrees_north]"},
    "time_ISO8601": {"ref": "yyyy-mm-ddThh:mm"},
    #use later in header https://vocab.nerc.ac.uk/collection/P01/current/TEMPP901/
    "Temperature Sea [~^o~#C]": {"src": "Temperature °C (Sea)"},
    "Temperature Air [~^o~#C]": {"src": "Temperature °C (Air)"},
    #https://vocab.nerc.ac.uk/collection/P01/current/PHXXZZXX/
    "pH": {"src": "pH-value"},
    #https://vocab.nerc.ac.uk/collection/P01/current/PSALZZXX/
    "Practical Salinity": {"src": "Salinity (‰)"},
    "Wind Speed [m/s]": {"src": "Wind speed (m/s)"},
    "Illuminance [lux]": {"src": "Light intensity (lux)"},
    "Secchi depth [m]": {"src": "Secchi depth (m)"},
    "Euphotic zone (Secchi depth x 2) [m]": {"src": "Euphotic zone (Secchi depth x 2) (m)"},
    "1/2 Secchi depth [m]": {"src": "1/2 Secchi depth (m)"},
    "Color of Forel-Ule scale at 1/2 Secchi depth": {"src": "Color of Forel-Ule scale at 1/2 Secchi depth"},
    "Time TNW": {"src": "Time TNW"},    
    "Weather": {"src": "Weather"},
    "Group": {"src": "Group"},
    "Comment": {"src": "Comment"},
    "Measurement instrument": {"src": "Measurement instrument"}
}


###take the xlsx sheet and loop over the SPEC and apply rules -> association xlsx to odv
def transform_one_file(df_in: pd.DataFrame, sheet_name: str) -> pd.DataFrame:
    out = pd.DataFrame(index=df_in.index)

    for out_col, rule in SPEC.items():
        if "fn" in rule:
            out[out_col] = rule["fn"](df_in, sheet_name=sheet_name)
        elif "src" in rule:
            src = rule["src"]
            out[out_col] = df_in[src] if src in df_in.columns else pd.NA
        elif "ref" in rule:
            out[out_col] = out[rule["ref"]]
        elif "const" in rule:
            out[out_col] = rule["const"]
        else:
            out[out_col] = pd.NA
    return out


###write output + header
def write_odv_with_header(df_out, header_path: Path, outfile: Path) -> None:
    header_text = header_path.read_text(encoding="utf-8")

    #pre
    # df_out.to_csv(
    #     outfile,
    #     sep="\t",
    #     index=False,
    #     header=True,
    #     mode="w",
    #     encoding="utf-8",
    #     lineterminator="\n",
    # )

    
    #1) write the header (overwrite any existing output)
    outfile.write_text(header_text.rstrip("\n") + "\n", encoding="utf-8")

    #2) append the data table (no header line from pandas)
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
    for name in SHEET_NAMES:
        print(f"proceccing sheet: {name}")
        df_in = pd.read_excel(DATA_DIR / INPUT_XLSX, sheet_name=name) 
        df_in.columns = df_in.columns.str.strip() # remove space
        rows.append(transform_one_file(df_in, sheet_name=name))
        #print(df_in.head())
        
    df_out = pd.concat(rows, ignore_index=True)

    #print(df_out.head())

    ###write to disc
    write_odv_with_header(df_out, HEADER_FILE, OUTFILE)




if __name__ == "__main__":
    main()
