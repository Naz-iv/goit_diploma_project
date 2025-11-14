import pytest
from models.models import DPOINT, TOOL, FSL, UTILITY, FRAME_REQUEST

def test_dpoint_initialization():
    dpoint = DPOINT(
        name="Gamma",
        datpid="101",
        length="4",
        is_mwd="1",
        ltb_addr="10",
        description="Gamma ray measurement"
    )
    
    assert dpoint.datpid == 101
    assert dpoint.length == 4
    assert dpoint.is_mwd is True
    assert dpoint.ltb_addr == 10
    assert dpoint.name == "Gamma"
    assert dpoint.description == "Gamma ray measurement"


def test_dpoint_equality():
    dpoint1 = DPOINT(name="Gamma", datpid=101, length=4, is_mwd=True, ltb_addr=10, description="Test")
    dpoint2 = DPOINT(name="Gamma", datpid=101, length=4, is_mwd=True, ltb_addr=10, description="Test")
    dpoint3 = DPOINT(name="Gamma", datpid=102, length=4, is_mwd=True, ltb_addr=10, description="Test")

    assert dpoint1 == dpoint2
    assert dpoint1 != dpoint3


def test_tool_initialization():
    tool = TOOL(
        tool_id="MWD_001",
        display_name="MWD Tool",
        ltb_addr="20",
        is_mwd="1",
        dpoint_suffix="_mwd",
        version="1.0"
    )
    
    assert tool.tool_id == "MWD_001"
    assert tool.display_name == "MWD Tool"
    assert tool.ltb_addr == 20
    assert tool.is_mwd is True
    assert tool.dpoint_suffix == "_mwd"


def test_tool_equality():
    tool1 = TOOL(tool_id="MWD_001", display_name="MWD Tool", ltb_addr=20, is_mwd=True, dpoint_suffix="_mwd", version="1.0")
    tool2 = TOOL(tool_id="MWD_001", display_name="MWD Tool", ltb_addr=20, is_mwd=True, dpoint_suffix="_mwd", version="1.0")
    tool3 = TOOL(tool_id="MWD_002", display_name="Another Tool", ltb_addr=21, is_mwd=False, dpoint_suffix="_lwd", version="2.0")

    assert tool1 == tool2
    assert tool1 != tool3


def test_fsl_initialization():
    fsl = FSL(
        description="Test FSL",
        bitrate="8",
        rop="150",
        nonorion_update="2",
        orion_update="5",
        R1_block=10,
        R1_space=2.5
    )

    assert fsl.description == "Test FSL"
    assert fsl.bitrate == 8
    assert fsl.rop == 150
    assert fsl.nonorion_update == 2
    assert fsl.orion_update == 5
    assert fsl.R1_block == 10
    assert fsl.R1_space == 2.5


def test_utility_initialization():
    utility = UTILITY(
        description="Utility Frame",
        bitrate="12",
        rop="200"
    )

    assert utility.description == "Utility Frame"
    assert utility.bitrate == 12
    assert utility.rop == 200


def test_frame_request_initialization():
    fsl1 = FSL(description="FSL1")
    fsl2 = FSL(description="FSL2")
    fsl3 = FSL(description="FSL3")
    fsl4 = FSL(description="FSL4")
    fsl5 = FSL(description="FSL5")
    fsl6 = FSL(description="FSL6")
    utility = UTILITY(description="Utility")

    frame_request = FRAME_REQUEST(
        uid="12345",
        job_number="J-001",
        well_name="Well-A",
        section_size="12.5",
        fsl1=fsl1,
        fsl2=fsl2,
        fsl3=fsl3,
        fsl4=fsl4,
        fsl5=fsl5,
        fsl6=fsl6,
        utility=utility,
        num_of_fsl=3,
        odf_required=True
    )

    assert frame_request.uid == "12345"
    assert frame_request.job_number == "J-001"
    assert frame_request.well_name == "Well-A"
    assert frame_request.section_size == "12.5"
    assert frame_request.num_of_fsl == 3
    assert frame_request.odf_required is True
    assert frame_request.fsl1.description == "FSL1"
    assert frame_request.utility.description == "Utility"
