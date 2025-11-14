import pytest
import xml.etree.ElementTree as ET
from datetime import datetime
import socket
from models import models
from hspm_version_mapper import QC_ELEMENT_MAPPER
from utils.export_to_fbw import convert_to_xml
from models.models import FRAME_REQUEST, FSL, TOOL, DPOINT, UTILITY


@pytest.fixture
def mock_frame_request():
    """Fixture to create a FRAME_REQUEST instance with mock dependencies."""

    # Mock DPOINT instances
    mock_dpoint1 = DPOINT(name="mwdstat", datpid=2541, length=6, is_mwd=True, ltb_addr=10, description="PowerUP RealTime status word")
    mock_dpoint2 = DPOINT(name="rgx", datpid=2513, length=12, is_mwd=True, ltb_addr=11, description="PowerUP Rotating Accelerometer X")

    # Mock TOOL instances
    mock_tool1 = TOOL(tool_id="DDR1TOOL", display_name="DDR1", ltb_addr=101, is_mwd=True, dpoint_suffix="X", version="9.4")
    mock_tool2 = TOOL(tool_id="DDR2TOOL", display_name="DDR2", ltb_addr=102, is_mwd=False, dpoint_suffix="Y", version="9.4")

    # Mock FSL instances
    mock_fsl = FSL(description="MAIN_DDR Config 1", bitrate=8, rop=70, mtf=[mock_dpoint1, mock_dpoint2])

    # Mock UTILITY instance
    mock_utility = UTILITY(description="Utility Config", bitrate=6, rop=100, utility=[mock_dpoint1])

    # Create FRAME_REQUEST instance using real instances of mocks
    frame_request = FRAME_REQUEST(
        uid="6b245f03-28cb-46ee-ad25-ae2d8a39c277",
        job_number="O.1047884.01",
        well_name="21_03a-H5Y",
        section_size="9.5in",
        fsl1=mock_fsl, fsl2=mock_fsl, fsl3=mock_fsl,
        fsl4=mock_fsl, fsl5=mock_fsl, fsl6=mock_fsl,
        utility=mock_utility,
        num_of_fsl=1,
        odf_required=True,
        modf_required=False,
        provision_bha=True,
        ddr_bha=False,
        hspm_version="RTDPointCatalog_2025_0.db",
        tools={mock_tool1, mock_tool2},
        odf_frame=[mock_dpoint1, mock_dpoint2],
        modf_frame=None,
        dds_frame=[mock_dpoint1]
    )

    return frame_request


def test_convert_to_xml_with_valid_data(mock_frame_request: FRAME_REQUEST):
    """Test convert_to_xml function with a valid frame request."""
    xml_output = convert_to_xml(mock_frame_request)
    
    assert xml_output is not None
    assert isinstance(xml_output, bytes)
    xml_output_str = xml_output.decode("utf-8")
    assert "<?xml " in xml_output_str
    
    assert f'TelemetryTool="DDR1TOOL" Version="9.4"' in xml_output_str
    assert f'Description="FSL-1" BitRate="{mock_frame_request.fsl1.bitrate}" ROP="{mock_frame_request.fsl1.rop}" Notes="{mock_frame_request.fsl1.description}"' in xml_output_str


def test_convert_to_xml_includes_fsl_data(mock_frame_request: FRAME_REQUEST):
    """Test if convert_to_xml includes FSL data properly."""
    xml_output = convert_to_xml(mock_frame_request).decode("utf-8")
    
    assert "<MagneticToolFaceFrame>" in xml_output
    assert "<GravityToolFaceFrame>" in xml_output
    assert "<RotatingFrame>" in xml_output
    

def test_convert_to_xml_contains_tool_information(mock_frame_request: FRAME_REQUEST):
    """Test convert_to_xml includes tool information."""
    xml_output = convert_to_xml(mock_frame_request).decode("utf-8")
    
    assert "<BottomHoleAssembly>" in xml_output
    
    for tool in mock_frame_request.tools:
        assert f'<Tool Name="{tool.tool_id}" Version="{tool.version}" ' in xml_output    
        

def test_convert_to_xml_use_correct_hspm_qc_element(mock_frame_request: FRAME_REQUEST):
    """Test convert_to_xml includes tool information."""
    xml_output = convert_to_xml(mock_frame_request).decode("utf-8")
    
    assert '<QCElement HSPMVersion="TnAShared2025_1_004"'  in xml_output
    
    mock_frame_request.hspm_version = None
    
    xml_output = convert_to_xml(mock_frame_request).decode("utf-8")
    
    assert '<QCElement HSPMVersion="TnAShared2023_1_001"'  in xml_output
