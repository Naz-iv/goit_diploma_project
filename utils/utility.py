import logging
import json
import re
import os

from dataclasses import asdict, is_dataclass
from models import models
from typing import List

from utils.export_to_fbw import convert_to_xml
from frame_builder import build_frames_from_json
from utils.db_managment import SQLHandler
from frame_library import FRAME_LIBRATY
from hspm_version_mapper import HSPM_VERSION_MAP


hspm_version = "RTDPointCatalog_2025_0.db"

def get_dpoint_list(
    dpoint_list: list,
    tools: List[models.TOOL],
    time_update: dict = None,
    depth_update: dict = None
) -> List[models.DPOINT]:
    """
    Return a filtered list of DPOINT objects based on provided dpoint_list, tools, and optional updates.

    This function processes an initial dpoint list and applies specific filtering rules to ensure data integrity. It then retrieves relevant
    DPOINT objects from the database based on the provided tools. Finally, it applies optional time and depth updates to the resulting
    DPOINT objects.

    Parameters:
        dpoint_list: list
            A list of dpoint names to filter and process.
        tools: List[models.TOOL]
            A list of TOOL objects whose ltb_addr properties are used to query the database.
        time_update: dict, optional
            A dictionary containing time updates keyed by dpoint name.
        depth_update: dict, optional
            A dictionary containing depth updates keyed by dpoint name.

    Returns:
        List[models.DPOINT]
            A list of filtered and processed DPOINT objects obtained after querying the database.
    """
    dpoints = []

    tool_name_list = [tool.display_name for tool in tools]
    
    # This is only for fixing issues existing in RTOF file, trying to remove dpoitns that should not be in RTOF!
    if ("RTA" in tool_name_list or "CLNK" in tool_name_list) and "TF_b" in dpoint_list:
        dpoint_list.pop(dpoint_list.index("TF_b"))
        dpoint_list.append("TFHI_b")
        
    if ("DDR_RT_2_r1" in dpoint_list or "DDR_RT_3_r1" in dpoint_list or "DDR_RT_4_r1" in dpoint_list) and ("DDR_RT_1_r1" in dpoint_list):
        dpoint_list.pop(dpoint_list.index("DDR_RT_1_r1"))
    
    if ("DDR_RT_2_r2" in dpoint_list or "DDR_RT_3_r2" in dpoint_list or "DDR_RT_4_r2" in dpoint_list) and ("DDR_RT_1_r2" in dpoint_list):
        dpoint_list.pop(dpoint_list.index("DDR_RT_1_r2"))
    
    if ("DDR_RT_2_r3" in dpoint_list or "DDR_RT_3_r3" in dpoint_list or "DDR_RT_4_r3" in dpoint_list) and ("DDR_RT_1_r3" in dpoint_list):
        dpoint_list.pop(dpoint_list.index("DDR_RT_1_r3"))
    
    if "C_PKPO4_s" in dpoint_list and "C_PKPO3_s" in dpoint_list:
        dpoint_list.pop(dpoint_list.index("C_PKPO4_s"))

    if ("GRCNTX_c" in dpoint_list or "GRCNT_c" in dpoint_list) and "O_grcnt_c" in dpoint_list:
        dpoint_list.pop(dpoint_list.index("O_grcnt_c"))

    if ("O_RHOB_v" in dpoint_list and "RHOB_v" in dpoint_list) or ("O_RHOB_v" in dpoint_list and "ROBB_v" in dpoint_list):
        dpoint_list.pop(dpoint_list.index("O_RHOB_v"))

    if ("O_DRHO_v" in dpoint_list and "DRHO_v" in dpoint_list) or ("O_DRHO_v" in dpoint_list and "DRHB_v" in dpoint_list):
        dpoint_list.pop(dpoint_list.index("O_DRHO_v"))

    if "CS3PK_H_mp" in dpoint_list and next(tool.version for tool in tools if tool.display_name == "MP3") in ["7.7", "8.4", "8.5", "8.7"]:
        if "SCLP_mp" not in dpoint_list:
            dpoint_list.append("SCLP_mp")

    if "CSSTAT_mp" in dpoint_list and next(
        tool.version for tool in tools if tool.display_name == "MP3"
        ) in ["8.5", "8.7"] and next(
            tool.size for tool in tools if tool.display_name == "MP3"
        ) in ["8.25", "6.75", "9.0"]:
        if "AET_mp" not in dpoint_list:
            dpoint_list.append("AET_mp")

    # if "O_ETNR_v" in dpoint_list and "O_LNNI_v" in dpoint_list and "O_SNNI_v" in dpoint_list:
    #     if "UCAV_v" in dpoint_list:
    #         dpoint_list.pop(dpoint_list.index("UCAV_v"))
    #     if "O_UCAV_v" not in dpoint_list:
    #         dpoint_list.append("O_UCAV_v")
    #
    # if "O_UCAV_v" in dpoint_list and float(next(
    #     tool.version for tool in tools if tool.display_name == "ECO"
    #     )) >= 8.2:
    #     dpoint_list.pop(dpoint_list.index("O_UCAV_v"))
    #     dpoint_list.append("O_UCAS_v")
    #
    # if "UCAV_v" in dpoint_list and float(next(
    #     tool.version for tool in tools if tool.display_name == "ECO"
    #     )) >= 8.2:
    #     dpoint_list.pop(dpoint_list.index("UCAV_v"))
    #     dpoint_list.append("UCAS_v")
    #
    # if "UCAV_v" in dpoint_list and "O_UCAS_v" in dpoint_list:
    #     dpoint_list.pop(dpoint_list.index("UCAV_v"))

    if "O_RBIT_gvr" in dpoint_list:
        dpoint_list.pop(dpoint_list.index("O_RBIT_gvr"))
        dpoint_list.append("RESBIT_gvr")
    # if "rgx" in tool_name_list and "C_rgx" in tool_name_list:
    #     dpoint_list.pop(dpoint_list.index("rgx"))
    # if "rhx" in tool_name_list and "C_rhx" in tool_name_list:
    #     dpoint_list.pop(dpoint_list.index("rhx"))
    
    with SQLHandler(hspm_version) as db:
        dpoints_query = db.select_dpoints_from_db_with_tools(
            dpoint_list,
            [tool.ltb_addr for tool in tools]
        )

    # Check if any result is returned
    if dpoints_query:
        # Iterate through each row in the result set
        for row in dpoints_query:
            dpoints.append(models.DPOINT(*row))
    else:
        logging.info("No data found.")

    if time_update or depth_update:
        for dpoint in dpoints:
            try:
                if dpoint.name in time_update:
                    dpoint.time = float(time_update.get(dpoint.name))
                if dpoint.name in depth_update and depth_update.get(dpoint.name):
                    dpoint.depth = float(depth_update.get(dpoint.name))    
            except (ValueError, TypeError) as e:
                logging.info(f"Failed to convert custom udpate for {dpoint.name}")
                continue

    return dpoints

def get_ordered_dpoint_list(
    dpoint_list: list,
    tools: List[models.TOOL],
) -> List[models.DPOINT]:
    """
    Fetches and returns an ordered list of DPOINT objects based on the provided dpoint names and
    tool instances. It interacts with the database to retrieve matching DPOINT data for the
    specified tools and ensures the order of the returned list matches the order of the
    provided dpoint_list.

    Parameters:
        dpoint_list: list
            A list of strings representing the names of dpoints to be retrieved and ordered.
        tools: List[models.TOOL]
            A list of TOOL objects for which the dpoints are queried.

    Returns:
        List[models.DPOINT]
            An ordered list of DPOINT objects matching the names present in dpoint_list.

    Raises:
        None
    """
    dpoints = []
    unique_dpoints = []

    with SQLHandler(hspm_version) as db:
        dpoints_query = db.select_dpoints_from_db_with_tools(
            dpoint_list,
            [tool.ltb_addr for tool in tools]
        )

    # Check if any result is returned
    if dpoints_query:
        # Iterate through each row in the result set
        for row in dpoints_query:
            unique_dpoints.append(models.DPOINT(*row))
    else:
        logging.info("No data found.")

    for dpoint_string in dpoint_list:
        temp = next(x for x in unique_dpoints if dpoint_string == x.name)
        if temp:
            dpoints.append(temp)
            
    return dpoints


def get_lwd_version(tool_name: str, tools_data: list) -> str:
    """
        Extracts and returns the version of a specified tool from a list of tool
        data. The function scans for a matching tool name and retrieves its
        version formatted as 'major.minor' if available. If no match or version
        is found, the function returns None.

        Parameters:
        tool_name: str
            The name of the tool whose version is to be extracted.
        tools_data: list
            A list of dictionaries containing tool data, where each dictionary
            includes tool attributes like 'NAME' and 'VERSION'.

        Returns:
        str
            The version of the specified tool in the format 'major.minor', or
            None if the tool or its version is not found.

        Raises:
        None
    """
    for tool in tools_data:
        if tool.get("NAME") == tool_name:
            match = re.search("v\d+\.\d+", tool.get("VERSION", ""))
            if match:
                return match.group(0).lstrip("v")
  
    return


def get_mwd_version(tool_name: str, tools_data: list) -> str:
    """
        Parse and extract the first version number of a tool from a dataset.

        This function scans a list of tool data dictionaries, identifies the
        tool with a matching name, and retrieves its version formatted as a
        string. The version number is extracted based on a standard pattern
        recognizing vX.Y, and only the first major version is considered
        with an appended "0". If no matching tool name or version is found,
        the function returns None.

        Parameters:
        tool_name : str
            The name of the tool to find within the dataset.
        tools_data : list
            A list of dictionaries containing tool metadata. Each dictionary
            should include the tool's "NAME" and "VERSION" keys.

        Returns:
        str
            The formatted version string of the specified tool if found,
            following the "X0" pattern, or None if no match is located.
    """
    for tool in tools_data:
        if tool.get("NAME") == tool_name:
            version_pattern = re.compile("v\s*(\d+)(?:\.(\d+))?", re.IGNORECASE)
            match = version_pattern.search(tool.get("VERSION", ""))
            if match:
                return match.group(1) + "0"
  
    return


def get_size(section_size: float) -> str:
    """
    Determine the size category based on the given section size.

    The function evaluates the input section size and categorizes it into one of
    several predetermined size labels. It returns a string representation of the
    corresponding size.

    Parameters:
        section_size (float): The size value to categorize.

    Returns:
        str: A string representing the size category.
    """
    if section_size <= 7.5:
        return "4.75"
    elif 7.5 < section_size <= 10:
        return "6.75"
    elif 10 < section_size <= 15:
        return "8.25"
    else:
        return "9.0"


def get_tools_list(
    tools_data: list,
    section_size: float
) -> List[models.TOOL]:
    """
    Fetches a list of tools from the database, processes them to populate additional
    attributes such as version and size, and returns the finalized list of tools.

    Attributes:
        tools_query (list): Contains the database query results for the requested
        tools.

    Args:
        tools_data: A list of dictionaries, each containing details about specific
        tools, including their names.
        section_size: A float value representing the size of the current section
        being processed.

    Returns:
        List[models.TOOL]: A list of TOOL objects with populated attributes
        including version and size.

    Raises:
        This function does not explicitly handle or raise exceptions but assumes
        proper handling of any database errors or related runtime exceptions
        externally.
    """
    tools = []
    
    with SQLHandler(hspm_version) as db:
        tools_query = db.select_tools_from_db([x.get("NAME") for x in tools_data])

    # Check if any result is returned
    if tools_query:
        # Iterate through each row in the result set
        for row in tools_query:
            tools.append(models.TOOL(*row))
    else:
        logging.info("No data found.")
    
    for tool in tools:
        if tool.is_mwd:
            rtof_version = get_mwd_version(
                tool.display_name,
                tools_data
            )
            if rtof_version:
                if tool.tool_id == "DVDXTTOOL" and rtof_version == "80":
                    tool.version = "100"
                else:
                    tool.version = str(rtof_version)
            else:
                with SQLHandler(hspm_version) as db:
                    tool.version = str(db.select_tool_latest_version_from_db(tool.tool_id))
        else:
            tool.version = get_lwd_version(
                tool.display_name,
                tools_data
            )
            tool.size = get_size(section_size)
            
    return tools


def update_ddr_tools(tools: List[models.TOOL], data: dict) -> None:
    """
        Updates DDR tools with spacing and block size information from the input data.

        The function iterates through a list of tools and updates their tr_spacing and
        rt_blocksize attributes based on the provided configuration data. The updates depend
        on the tool_id and its corresponding settings in the given data dictionary. For tools
        with "DDRT" in their tool_id, the version is updated to match the version of the last
        DDR tool processed.

        Parameters:
        tools: List[models.TOOL]
            A list of TOOL objects to update. The TOOL objects must have tool_id, version,
            tr_spacing, and rt_blocksize attributes to be updated.
        data: dict
            A dictionary containing the configuration data. The dictionary must include
            nested structure for keys 'fsl1', with possible subkeys 'R1_space', 'R1_block',
            'R2_space', 'R2_block', 'R3_space', and 'R3_block'.

        Returns:
        None
    """
    ddr_version = ""
    # Update DDR with spacings and block size
    for tool in tools:
        if "DDR1" in tool.tool_id:
            tool.tr_spacing = data.get("fsl1", {}).get("R1_space")
            tool.rt_blocksize = data.get("fsl1", {}).get("R1_block")
            ddr_version = tool.version
        elif "DDR2" in tool.tool_id:
            tool.tr_spacing = data.get("fsl1", {}).get("R2_space")
            tool.rt_blocksize = data.get("fsl1", {}).get("R2_block")
        elif "DDR3" in tool.tool_id:
            tool.tr_spacing = data.get("fsl1", {}).get("R3_space")
            tool.rt_blocksize = data.get("fsl1", {}).get("R3_block")
        elif "DDRT" in tool.tool_id:
            tool.version = ddr_version


def get_fsl_data(
    fsl_data: dict,
    tools: list
    ) -> models.FSL:
    """
    Processes and returns an FSL model populated with the provided FSL data and tools.

    The function computes the time and depth updates for various drilling data points, and it processes
    frames associated with MTF (Mechanical Tool Face), GTF (Geological Tool Face), and ROT (Rotary data).
    It uses specific update rules based on parameters in the input data dictionary, including settings for
    NMR (Nuclear Magnetic Resonance) and TF (Tool Face frequencies), and applies custom spacing and temporal
    updates for different DDR (Drilling Data Recorder) spacing configurations such as R1, R2, and R3.

    Parameters:
    fsl_data: dict
        Input dictionary containing various data points and configuration parameters necessary
        for calculating updates and constructing the FSL model.
    tools: list
        List of tools used in the construction and computation of the FSL model.

    Returns:
    models.FSL
        A newly constructed FSL object populated with the processed time and depth data points,
        including MTF, GTF, and Rotary sets.
    """
    logging.info("Parsing FLS data")

    time_update = {item["Name"]: item["Update"] for item in fsl_data.get("time")} if fsl_data.get("time") else {}
    depth_update = {item["Name"]: item["Update"] for item in fsl_data.get("depth")} if fsl_data.get("depth") else {}

    mtf_dpoints = json.loads(fsl_data.get("MTF"))
    gtf_dpoints = json.loads(fsl_data.get("GTF"))
    rot_dpoints = json.loads(fsl_data.get("ROT"))
    
    """
    DDR Spacing update formulas:
    R1:
        x = fsl_data.get("R1_space") / (5 * fsl_data.get("R1_block"))
        time_update[dpoint] = 6.56 * 3600 / (fsl_data.get("ROP") * fsl_data.get("R1_block"))
        depth_update[dpoint] = x if x > 0.25 else 0.25
    R2:
        time_update[dpoint] = 8.2 * 3600 / (fsl_data.get("ROP") * fsl_data.get("R2_block"))
        depth_update[dpoint] = fsl_data.get("R2_space") / (5 * fsl_data.get("R2_block"))
    R3:
        time_update[dpoint] = 13.12 * 3600 / (fsl_data.get("ROP") * fsl_data.get("R3_block"))
        depth_update[dpoint] = fsl_data.get("R3_space") / (5 * fsl_data.get("R3_block"))
    """
    
    if fsl_data.get("NMR"):
        for dpoint in rot_dpoints:
            if dpoint in ["O_T2MRP_m", "O_T2SV1_m", "O_T2SV2_m", "T2LM2_m", "MRP2C_m", "MRF2C_m","BFV2C_m"]:
                time_update[dpoint] = int(fsl_data.get("NMR")) * 1.15
            if dpoint in ["O_MR_PRJ8_m4", "O_MR_PRJ8_m4", "O_MR_PRJ8_m4", "O_MR_PRJ8_m4"]:
                time_update[dpoint] = int(fsl_data.get("NMR")) * 4 # since MagniSphere block update is provided for 4 dpoints, average update is multiplied by 4

    is_raw_tf_frame = False

    for dpoint in mtf_dpoints[:]:
        if dpoint in ["mtf", "mtfs"]:
            time_update[dpoint] = int(fsl_data.get("TF"))
        if dpoint in ["ITFS_g", "GTFS_g", "CO_ITF_gdi"]:
            time_update[dpoint] = int(fsl_data.get("TF"))
        if dpoint in ["hz", "hy", "gy", "gz", "rhz", "rhy", "rgy", "rgz"]:
            mtf_dpoints = [dp for dp in mtf_dpoints if dp not in ["mtf", "mtfs"]]
            time_update[dpoint] = int(fsl_data.get("TF")) * 4  #since raw TF come in set of four dpoints update is multiplied by 4
            is_raw_tf_frame = True
    
    for dpoint in gtf_dpoints:
        if dpoint in ["gtf", "gtfs"]:
            time_update[dpoint] = int(fsl_data.get("TF"))
             
    mtf = get_dpoint_list(
        mtf_dpoints,
        tools,
        time_update,
        depth_update
    )
    gtf = get_dpoint_list(
        gtf_dpoints,
        tools,
        time_update,
        depth_update
    )
    
    dpoints_to_skip_in_rot = ["GEN1_p", "GEN13_p", "ANNPRESS_p"]
    
    # TODO: temporary fix for TST non-ODF frame generation. Fix: allow users to specify requered updare for mtf/gtf/rot frames
    time_update = {key: value for key, value in time_update.items() if key not in dpoints_to_skip_in_rot}

    rot = get_dpoint_list(
        rot_dpoints,
        tools,
        time_update,
        depth_update
    )

    logging.info("Creating FSL")

    fsl = models.FSL(
        description = fsl_data.get("description"),
        bitrate = fsl_data.get("bitrate"),
        rop = fsl_data.get("ROP"),
        nonorion_update = fsl_data.get("nonORION"),
        orion_update = fsl_data.get("ORION"),
        mtf = mtf,
        gtf = gtf,
        rotary = rot,
        R1_block = fsl_data.get("R1_block") if fsl_data.get("R1_block") else 2,
        R1_space = fsl_data.get("R1_space") if fsl_data.get("R1_space") else 100,
        R2_block = fsl_data.get("R2_block") if fsl_data.get("R2_block") else 2,
        R2_space = fsl_data.get("R2_space") if fsl_data.get("R2_space") else 200,
        R3_block = fsl_data.get("R3_block") if fsl_data.get("R3_block") else 2,
        R3_space = fsl_data.get("R3_space") if fsl_data.get("R3_space") else 300,
    )


    #TODO: Raw TF in MWD only frames requires increase max_bounds_pct to space out dpoints
    if is_raw_tf_frame and len(fsl.mtf) < 20:
        fsl.max_bounds_pct = 70

    logging.info("FSL created OK")

    return fsl


def get_frameset_tools(data: dict) -> list:
    """
        Extracts and compiles a list of tools from a given data dictionary.

        This function iterates through specific key patterns in the data
        and accumulates tool information by parsing the relevant JSON data.
        The result is a consolidated list of tools extracted from all applicable
        keys in the provided dictionary.

        Args:
            data (dict): A dictionary containing potential key-value pairs
            with tool information.

        Returns:
            list: A list containing all extracted tools, merged from all relevant
            keys within the input dictionary.
    """
    tools = []
    for i in range(1, 7):
        temp_fsl = data.get(f"fsl{i}")
        if temp_fsl:
            current = json.loads(temp_fsl.get("TOOLS"))
            if current:
                tools += current

    return tools


def get_frameset_utility_dpoints(data: dict) -> list:
    """
    Processes utility datapoints from a dictionary containing serialized
    frame set utility data. The method extracts utility data for up to six
    frame sets, deserializes the data and compiles a unique list of all
    utility datapoints.

    Args:
        data (dict): A dictionary containing serialized utility data for
        multiple frame sets. Each key follows the format 'fsl{n}', where n
        is the frame set index. Values are dictionaries containing the
        key "UTIL" with JSON-encoded utility data as a string.

    Returns:
        list: A unique list of utility datapoints aggregated from the
        deserialized utility data.
    """
    dpoints = []
    for i in range(1, 7):
        temp_util = data.get(f"fsl{i}")
        if temp_util:
            current = json.loads(temp_util.get("UTIL"))
            if current:
                dpoints += current

    return list(set(dpoints))


def update_fsl(frame_set: models.FSL, fsl_data: dict, tools: list) -> models.FSL:
    """
    Updates the attributes of a provided FSL object with values from the given data.

    This function takes an `FSL` object, a dictionary containing data for updates, and
    a list of tools. It updates the attributes of the `FSL` object using the provided
    data. If a value is not provided in the data dictionary, the original value in
    the `FSL` object is retained.

    Parameters:
        frame_set: An instance of `models.FSL`, representing the object to be updated.
        fsl_data: A dictionary containing new data to update the `FSL` attributes. Keys
            expected in the dictionary include `Parameters` (with subkey `ROP`), `mtf`,
            `gtf`, and `rotary`.
        tools: A list of tools utilized for ordering data points in certain attributes
            (`mtf`, `gtf`, `rotary`).

    Returns:
        Returns the updated `models.FSL` object with attributes modified based on the
        provided data.

    Raises:
        Does not explicitly raise exceptions, but relies on the robustness of `get`
        operations and assumptions about the structure of `fsl_data`.
    """
    frame_set.rop = fsl_data.get("Parameters", frame_set.rop).get("ROP", frame_set.rop)
    frame_set.mtf = get_ordered_dpoint_list(fsl_data.get("mtf", frame_set.mtf), tools)
    frame_set.gtf = get_ordered_dpoint_list(fsl_data.get("gtf", frame_set.gtf), tools)
    frame_set.rotary = get_ordered_dpoint_list(fsl_data.get("rotary", frame_set.rotary), tools)

    return frame_set


def update_utility(utility_frame: models.UTILITY, dpoint_list: list, tools: list) -> models.UTILITY:
    """
    Updates the utility attribute of a given utility_frame object based on the ordered
    data points derived from the provided dpoint_list and tools. The function modifies
    the utility_frame object in-place.

    Args:
        utility_frame (models.UTILITY): The target object with a utility attribute to
            be updated.
        dpoint_list (list): A list of data points used to generate the ordered list.
        tools (list): A list of tools utilized for ordering the data points.

    Returns:
        models.UTILITY
    """
    utility_frame.utility = get_ordered_dpoint_list(dpoint_list, tools)
    return utility_frame


def main(data: dict) -> tuple:
    """
    Generates a frame request for given data input and processes the request through multiple
    steps including validation, frame generation, error handling, and database updates.

    Parameters
    ----------
    data : dict
        The input data containing configuration details and attributes for the frame request.
        This includes job metadata, well details, tool information, and frame-specific settings.

    Returns
    -------
    str
        The serialized XML string representation of the generated and processed frame request.

    Raises
    ------
    ValueError
        If frame generation fails due to incompatible parameters or tool configurations.

    Notes
    -----
    The function performs the following key steps:
    1. Tool and feature extraction: Processes tool data based on input details, extracting tool
       lists and relevant features required for the frame request.
    2. Request object setup: Constructs an instance of FRAME_REQUEST populated with provided
       and derived inputs.
    3. Conditional processing: Handles specific properties of the frame request (e.g., ODF, MODF)
       based on respective feature requirements across available tools.
    4. File operations: Writes the frame request to an input JSON file and saves the processing
       output as an output JSON file.
    5. Validation and error handling: Extracts and processes errors, either handling them internally
       or raising appropriate exceptions for further troubleshooting.
    6. XML conversion and database update: Converts the configured frame request object into XML
       format and updates it in the database for permanent storage.

    Examples
    --------
    Usage of this function requires valid configurations for `data`. An exception is raised
    if the frame generator encounters critical errors that cannot be auto-resolved.
    """
    logging.info("Processing tools information")
    tools = get_tools_list(get_frameset_tools(data), float(data.get("section_size").rstrip("in")))   
    mwd_tool = next((tool for tool in tools if tool.is_mwd), None)
    if not mwd_tool:
        logging.info("No MWD tool in frame request")
        raise ValueError("MWD tool is missing in frame request!")
        
    logging.info("Tools information processed successfully")
    
    if data.get("ddr_bha"):
        update_ddr_tools(tools, data)
    
    global hspm_version

    logging.info("Gettign HSPM version form user")    
    hspm_version_from_user = HSPM_VERSION_MAP.get(data.get("hspm_version"))
    if hspm_version_from_user:
        hspm_version = hspm_version_from_user
    
    logging.info("Creating Frame Request")

    frame_request = models.FRAME_REQUEST(
        uid = data.get("uid"),
        job_number = data.get("job_number"),
        well_name = data.get("well_name"),
        section_size = data.get("section_size"),
        fsl1 = get_fsl_data(data.get("fsl1"), tools) if data.get("fsl1") else models.FSL(),
        fsl2 = get_fsl_data(data.get("fsl2"), tools) if data.get("fsl2") else models.FSL(),
        fsl3 = get_fsl_data(data.get("fsl3"), tools) if data.get("fsl3") else models.FSL(),
        fsl4 = get_fsl_data(data.get("fsl4"), tools) if data.get("fsl4") else models.FSL(),
        fsl5 = get_fsl_data(data.get("fsl5"), tools) if data.get("fsl5") else models.FSL(),
        fsl6 = get_fsl_data(data.get("fsl6"), tools) if data.get("fsl6") else models.FSL(),
        num_of_fsl = int(data.get("num_of_fsl")),
        odf_required = False, #  if data.get("odf_required") == "true" else False,
        modf_required = True if data.get("modf_required") == "true" else False,
        provision_bha = True if data.get("provision_bha") == "true" else False,
        ddr_bha = True if data.get("ddr_bha") == "true" else False,
        utility = models.UTILITY(
            description = data.get("fsl1", {}).get("description"),
            bitrate = data.get("fsl1", {}).get("bitrate"),
            rop = data.get("fsl1", {}).get("ROP"),
            utility = get_dpoint_list(get_frameset_utility_dpoints(data), tools)
        ),
        tools = tools,
        hspm_version=hspm_version
    )
    
    logging.info("Frame Request created OK")

    if "ROSTOOL" == mwd_tool.tool_id:
        frame_request.dds_frame = get_ordered_dpoint_list(FRAME_LIBRATY.DDS.get(mwd_tool.version), tools)
        for fsl in [frame_request.fsl1, frame_request.fsl2, frame_request.fsl3, frame_request.fsl4, frame_request.fsl5, frame_request.fsl6]:
            fsl.max_dpoints = 333

    if "DVDXTTOOL" == mwd_tool.tool_id and frame_request.num_of_fsl > 3:
        frame_request.num_of_fsl = 3

    odf = []
    for tool in tools:
        temp = FRAME_LIBRATY.ODF.get(tool.tool_id, {}).get(tool.version, [])
        if odf:
            unique = set(odf)
            for item in temp:
                if item not in unique:
                    odf.append(item)
        else:
            odf = temp
    if odf:
        frame_request.odf_required = True
        frame_request.odf_frame = get_ordered_dpoint_list(odf, tools)

    logging.info("ODF added OK")

    if frame_request.modf_required:
        for tool in tools:
            modf = FRAME_LIBRATY.MODF.get(tool.tool_id, {}).get(tool.version)
            if modf:
                for modf_set in modf:
                    frame_request.modf_frame.append(get_ordered_dpoint_list(modf_set, tools))
                
                break
    logging.info("MODF added OK")

    input_file = os.path.join(os.getcwd(), f"input.json")

    with open(input_file, "w") as file:
        json.dump(asdict(frame_request), file, indent=4)

    logging.info("Sent request to frame generator")

    # Frame generator
    frame_generator_output, errors = build_frames_from_json(asdict(frame_request))

    logging.info("Frames generated")

    output_file = os.path.join(os.getcwd(), f"output.json")

    with open(output_file, "w") as file:
        json.dump(frame_generator_output, file, indent=4)

    #TODO: Update error handling to mention which FSLs failed to build to make it easy for users to re-submit
    errors = {fsl: msg for fsl, sub_dict in errors.items() for msg in sub_dict.values() if msg}

    logging.info("Checking for errors")

    status = 200

    if not errors:
        fsl1 = frame_generator_output.get("fsl1")
        if fsl1:
            if fsl1.get("Parameters").get("ROP") != frame_request.fsl1.rop:
                status = 209
            frame_request.fsl1 = update_fsl(frame_request.fsl1, fsl1, tools)
        
        fsl2 = frame_generator_output.get("fsl2")
        if fsl2:
            if fsl2.get("Parameters").get("ROP") != frame_request.fsl2.rop:
                status = 209
            frame_request.fsl2 = update_fsl(frame_request.fsl2, fsl2, tools)
        
        fsl3 = frame_generator_output.get("fsl3")
        if fsl3:
            if fsl3.get("Parameters").get("ROP") != frame_request.fsl3.rop:
                status = 209
            frame_request.fsl3 = update_fsl(frame_request.fsl3, fsl3, tools)
        
        fsl4 = frame_generator_output.get("fsl4")
        if fsl4:
            if fsl4.get("Parameters").get("ROP") != frame_request.fsl4.rop:
                status = 209
            frame_request.fsl4 = update_fsl(frame_request.fsl4, fsl4, tools)
        
        fsl5 = frame_generator_output.get("fsl5")
        if fsl5:
            if fsl5.get("Parameters").get("ROP") != frame_request.fsl5.rop:
                status = 209
            frame_request.fsl5 = update_fsl(frame_request.fsl5, fsl5, tools)
        
        fsl6 = frame_generator_output.get("fsl6")
        if fsl6:
            if fsl6.get("Parameters").get("ROP") != frame_request.fsl6.rop:
                status = 209
            frame_request.fsl6 = update_fsl(frame_request.fsl6, fsl6, tools)
        
        utility = frame_generator_output.get("utility", {}).get("utility")
        if utility:
            frame_request.utility = update_utility(frame_request.utility, utility, tools)
    else:
        failed_to_build = [f'{fsl.upper()}: {msg}' for fsl, msg in errors.items()]
        tool_not_compatible = [msg for fsl, msg in errors.items() if 'Rules for ' in msg]

        if failed_to_build:
            raise ValueError('\n'.join(failed_to_build))
        elif tool_not_compatible:
            raise ValueError(f"{tool_not_compatible[0].split(' (no ')[0]}. Tool is not compatible with Frame Generator.")
        else:
            raise ValueError(f"{' | '.join([f'{fsl.upper()} - {msg}'for fsl, msg in errors.items()])}")

    xml_string = convert_to_xml(frame_request)
    
    logging.info("Frames converted to xml")

    with SQLHandler(hspm_version) as db:
        db.update_xml_in_sqlite(data.get("uid"), xml_string, status)
        
    return xml_string, status
