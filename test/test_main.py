import pytest
from pathlib import Path
import json
from unittest.mock import MagicMock

from frame_builder.api import build_frames_from_json
from utils.utility import (
    get_dpoint_list,
    get_ordered_dpoint_list,
    get_lwd_version,
    get_mwd_version,
    get_size,
    get_tools_list,
    update_ddr_tools,
    get_fsl_data,
    get_frameset_tools,
    get_frameset_utility_dpoints,
    update_fsl,
    update_utility,
    main,
)
from models import models
from utils.db_managment import SQLHandler

# Mock Data
@pytest.fixture
def mock_tools():
    """Returns a list of mock TOOL objects"""
    return [
        models.TOOL(tool_id="IMPTOOL", display_name="IMP", ltb_addr="137", is_mwd=True, version="160", dpoint_suffix=""),
        models.TOOL(tool_id="MXWPTOOL", display_name="MXWP", ltb_addr="19", is_mwd=False, version="10.0", dpoint_suffix="_mx"),
    ]


@pytest.fixture
def mock_request_data():
    return {
        "ddr_bha": "true",
        "fsl1": {
            "GTF": "[\"ajamcnt\",\"di_temp\",\"O_grav_mx\",\"O_ra40h_mx\"]",
            "MTF": "[\"ajamcnt\",\"di_temp\",\"O_grav_mx\",\"O_ra40h_mx\"]",
            "NMR": 40,
            "ORION": 6,
            "R1_block": 15,
            "R1_space": 31,
            "R2_block": 30,
            "R2_space": 31,
            "R3_block": 50,
            "R3_space": 31,
            "ROP": 80,
            "ROT": "[\"ajamcnt\",\"di_temp\",\"O_grav_mx\",\"O_ra40h_mx\"]",
            "RTOF": "O.1011090.10_dwdw_12.25in_RTOF.json",
            "TF": 40,
            "TOOLS": "[{\"NAME\":\"IMP\",\"VERSION\":\"v16\",\"LONG\":\"Impulse\"},{\"NAME\":\"MXWP\",\"VERSION\":\"v10\",\"LONG\":\"PeriScope\"}]",
            "UTIL": "[\"ajamcnt\",\"di_temp\",\"O_grav_mx\",\"O_ra40h_mx\"]",
            "bitrate": 6,
            "depth": [],
            "description": "Test FSL",
            "nonORION": 1,
            "time": []
        },
        "fsl2": None,
        "fsl3": None,
        "fsl4": None,
        "fsl5": None,
        "fsl6": None,
        "job_number": "O.1001098.10",
        "modf_required": "true",
        "num_of_fsl": "2",
        "odf_required": "true",
        "provision_bha": "true",
        "section_size": "12.25in",
        "uid": "c6968bf2-8cb7-4b6c-a766-13230bb9139b",
        "well_name": "SF-90", 
        "hspm_version": "RTDPointCatalog_2025_0.db"
    }

@pytest.fixture
def mock_frame_request():
    # Load the JSON data
    json_path = Path(__file__).parent / "test_input_data.json"
    with open(json_path, "r") as f:
        data = json.load(f)

    # Convert it back to the FSL instance
    return data

# Tests
def test_get_size():
    assert get_size(7.0) == "4.75"
    assert get_size(8.0) == "6.75"
    assert get_size(12.0) == "8.25"
    assert get_size(16.0) == "9.0"

def test_get_lwd_version():
    tools_data = [{"NAME": "ECO", "VERSION": "v7.1"}]
    assert get_lwd_version("ECO", tools_data) == "7.1"
    assert get_lwd_version("LWD2", tools_data) is None

def test_get_mwd_version():
    tools_data = [{"NAME": "IMP", "VERSION": "v 16.0"}]
    assert get_mwd_version("IMP", tools_data) == "160"
    assert get_mwd_version("MWD2", tools_data) is None

def test_get_tools_list():
    tools_data = [{"NAME": "IMP", "VERSION": "v16.0"}, {"NAME": "MXWP", "VERSION": "v16.0"}]
    section_size = 9.0
    tools = get_tools_list(tools_data, section_size)
    assert len(tools) == 2
    assert tools[0].tool_id == "IMPTOOL"
    assert tools[1].tool_id == "MXWPTOOL"
    assert tools[0].ltb_addr == 137
    assert tools[1].ltb_addr == 19


def test_get_dpoint_list(mock_tools):
    dpoints = get_dpoint_list(["rgx", "sticknslip", "SHKLV_mx", "O_rp28h_mx"], mock_tools)

    assert len(dpoints) == 4
    for dpoint in dpoints:
        assert dpoint.name in ["rgx", "sticknslip", "SHKLV_mx", "O_rp28h_mx"]
        assert dpoint.datpid in [4857, 4504, 4922, 4650]

def test_get_ordered_dpoint_list(mock_tools):
    dpoints = get_ordered_dpoint_list(["rgx", "sticknslip", "SHKLV_mx", "O_rp28h_mx"], mock_tools)
    assert len(dpoints) == 4
    for dpoint, name, dpoint_id in zip(dpoints, ["rgx", "sticknslip", "SHKLV_mx", "O_rp28h_mx"], [4504, 4650, 4922, 4857]):
        assert dpoint.name == name
        assert dpoint.datpid == dpoint_id

def test_update_ddr_tools():
    tools = [
        models.TOOL(tool_id="DDR1TOOL", display_name="DDR1", ltb_addr="101", is_mwd=False, version="9.4",
                    dpoint_suffix="_r1"),
        models.TOOL(tool_id="DDR2TOOL", display_name="DDR2", ltb_addr="102", is_mwd=False, version="9.4",
                    dpoint_suffix="_r2"),
        models.TOOL(tool_id="DDR3TOOL", display_name="DDR3", ltb_addr="103", is_mwd=False, version="9.4",
                    dpoint_suffix="_r3")
    ]
    data = {"fsl1": {"R1_space": 100, "R1_block": 32,"R2_space": 150, "R2_block": 32,"R3_space": 200, "R3_block": 32}}
    update_ddr_tools(tools, data)
    for tool, block, spacing in zip(tools, [32, 32, 32], [100, 150, 200]):
        assert tool.tr_spacing == spacing
        assert tool.rt_blocksize == block

def test_get_fsl_data(mock_request_data, mock_tools):
    fsl = get_fsl_data(mock_request_data.get("fsl1"), mock_tools)
    assert fsl.description == "Test FSL"
    assert fsl.bitrate == 6
    assert fsl.rop == 80
    assert fsl.nonorion_update == 1
    assert fsl.orion_update == 6

def test_get_frameset_tools(mock_request_data):
    tools = get_frameset_tools(mock_request_data)
    assert len(tools) == 2

    assert tools[0]["NAME"] == "IMP"
    assert tools[1]["NAME"] == "MXWP"

def test_get_frameset_utility_dpoints(mock_request_data):
    dpoints = get_frameset_utility_dpoints(mock_request_data)
    assert len(dpoints) == 4

def test_update_fsl(mock_frame_request, mock_tools):
    frame_set = models.FSL(**mock_frame_request.get("fsl1"))

    new_data = {
        "Parameters": {
            "ROP": 120,
        },
        "mtf": ["ajamcnt", "di_temp", "O_grav_mx", "O_ra40h_mx"],
        "gtf": ["ajamcnt", "di_temp", "O_grav_mx", "O_ra40h_mx"],
        "rotary": ["ajamcnt", "di_temp", "O_grav_mx", "O_ra40h_mx"]}
    updated_fsl = update_fsl(frame_set, new_data, mock_tools)
    assert updated_fsl.rop == 120
    assert len(updated_fsl.mtf) == 4
    assert updated_fsl.mtf[0].name == "ajamcnt"

def test_build_frames_from_json(mock_frame_request):
    result, errors = build_frames_from_json(mock_frame_request)

    assert isinstance(result, dict)
    assert errors == {'fsl1': {'rotary': None, 'mtf': None, 'gtf': None}, 'utility': {'utility': None}}
    assert result != {}
