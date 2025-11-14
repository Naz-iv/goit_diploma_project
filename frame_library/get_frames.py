import os
import pandas as pd


DDS_FILEPATH = os.path.join(os.path.dirname, "DDS")
ODF_FILEPATH = os.path.join(os.path.dirname, "ODF")
MODF_FILEPATH = os.path.join(os.path.dirname, "MODF")


# Function to get values for a specific version
def get_dds_for_version(tool_name, version):
    df = pd.read_csv(os.path.join(DDS_FILEPATH, f"{tool_name}_DDS.csv"))
    data = {str(row['version']): row['values'].split(",") for _, row in df.iterrows()}
    return data.get(version, [])


# def get_odf_for_tool_and_version(tool_name, version):
#     data = read_csv_to_dict(os.path.join(ODF_FILEPATH, f"{tool_name}_ODF.csv"))
#     return data.get(version, [])


def get_modf_for_tool_and_version(tool_name, version):
    df = pd.read_csv(os.path.join(MODF_FILEPATH, f"{tool_name}_MODF.csv"))
    #TODO: Write MODF form data frame to a dixtianaty with key containign DHS version and value containing list of list of dpoints somethign like below
    data = {}
    data[version] = [dpoints for dpoints in df if df.version == version]
    return data.get(version, [])
