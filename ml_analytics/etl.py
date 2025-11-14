import pandas as pd
import ast
import sqlite3


def load_sqlite_to_df(sqlite_path: str, table_name: str = "Frame_Request_Data"):
    conn = sqlite3.connect(sqlite_path)
    df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
    conn.close()
    return df


def load_data(path: str) -> pd.DataFrame:
    df = load_sqlite_to_df(path)

    df = (df.drop(columns=["UniqueID", "HSPM version"]).rename(columns={
        "Required number of FSLs?": "Num_FSLs",
        "Processing Started At":"Start_Time",
        "Completed at":"End_Time",
        "GeoSphere in BHA": "GeoSphere",
        "TOOLS": "Tools"
        }))
    for col in ["Created", "Start_Time", "End_Time"]:
        if col in df.columns:
            # Try to parse with a known format first, fall back to auto
            try:
                df[col] = pd.to_datetime(df[col], format="%Y-%m-%dT%H:%M:%S")
            except Exception:
                df[col] = pd.to_datetime(df[col])

    df['Status'] = df['Status'].str.upper().map({
        'COMPLETED': 'success',
        'FAILED': 'error',
        'PROCESSING': 'error'
    }).fillna('Unknown')

    df = df[df['Tools'].notna() & (df['Tools'] != '')]

    df['Tools'] = df['Tools'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else []).apply(lambda tools: [tool['NAME'] for tool in tools])

    df['ToolCount'] = df['Tools'].apply(len)

    if "Duration" not in df.columns:
        df["Duration"] = pd.NA

    numeric_cols = ["Duration", "ToolCount", "Bitrate", "ROP"]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df
