import xml.etree.ElementTree as ET
from datetime import datetime
import socket
from models import models
from typing import List
from hspm_version_mapper import QC_ELEMENT_MAPPER


def convert_to_xml(frame_request: models.FRAME_REQUEST):
    """
    Convert the FRAME_REQUEST object into an XML representation.

    This function takes a `FRAME_REQUEST` object, processes its attributes and nested
    data structures, and generates an XML document. The generated XML representation
    follows a specified structure which includes elements like `Run`, `BottomHoleAssembly`,
    `FrameSetList`, `DDSFrame`, `UtilityFrame`, `OnDemandFrame`, and others, based on the
    input data.

    Detailed metadata, attributes, and content are assigned to each XML element. These are
    derived from properties of the `FRAME_REQUEST` object and its associated sub-elements,
    such as tools, frame sets, and data points. Additional computed values like naming conventions
    and default attributes are also utilized. The function ensures that the XML document maintains
    consistency and adheres to a specific schema.

    The resulting XML is encoded as a UTF-8 byte string. Clients of this function can use the
    string for further processing, including saving to a file or transferring it to external systems.

    Arguments:
        frame_request (models.FRAME_REQUEST): A structured input object containing the necessary
                                              data to generate the XML document.

    Returns:
        bytes: A UTF-8 encoded byte string representing the generated XML document.
    """
    date = datetime.today().date().isoformat()

    mwd_tool = next(tool for tool in frame_request.tools if bool(tool.is_mwd))
    # Root element 'Run' with attributes
    run_element = ET.Element(
        "Run",
        {
            "Name": f"{frame_request.job_number}_{frame_request.well_name}_{frame_request.section_size}_{date}",
            "TelemetryTool": str(mwd_tool.tool_id),
            "Version": str(mwd_tool.version),
            "UnitSystem": "ENGLISH",
            "UserMode": "FIELD",
            "FileDate": str(datetime.now().isoformat()),
            "LastEditComputer": socket.gethostname(),
            "NumberOfFSLToBeDisplayed": "6",
            "FileFormatVersion": "3.0",
        },
    )

    # BottomHoleAssembly with Tools
    bha_element = ET.SubElement(run_element, "BottomHoleAssembly")
    for tool in frame_request.tools:
        tool_element = ET.SubElement(bha_element, "Tool", {"Name": str(tool.tool_id)})
        if tool.version:
            tool_element.set("Version", str(tool.version))
        if tool.size:
            tool_element.set("Size", str(tool.size))
        if tool.tr_spacing:
            tool_element.set("TRSpacing", str(tool.tr_spacing))
        if tool.rt_blocksize:
            tool_element.set("RTBlockSize", str(tool.rt_blocksize))
            tool_element.set("RTBlockSize_A", str(tool.rt_blocksize))
            tool_element.set("RTBlockSize_B", str(tool.rt_blocksize))
            tool_element.set("RTBlockSize_C", str(tool.rt_blocksize))
            tool_element.set("DataComp", "false")
            tool_element.set("DataComp_A", "false")
            tool_element.set("DataComp_B", "false")
            tool_element.set("DataComp_C", "false")
            tool_element.set("CompDis", "0")
            tool_element.set("CompDis_A", "0")
            tool_element.set("CompDis_B", "0")
            tool_element.set("CompDis_C", "0")

    fsl_list = [
        frame_request.fsl1,
        frame_request.fsl2,
        frame_request.fsl3,
        frame_request.fsl4,
        frame_request.fsl5,
        frame_request.fsl6,
    ]
    if str(mwd_tool.tool_id) == "DVDXTTOOL":
        fsl_list = [frame_request.fsl1, frame_request.fsl2, frame_request.fsl3]
    if str(mwd_tool.tool_id) == "SPTOOL":
        fsl_list = [frame_request.fsl1]

    for i, fsl in enumerate(fsl_list):

        # FrameSetList
        fsl_element = ET.SubElement(
            run_element,
            "FrameSetList",
            {
                "Description": f"FSL-{i + 1}",
                "BitRate": str(fsl.bitrate),
                "ROP": str(fsl.rop),
                "Notes": fsl.description,
                "CheckStatus": "Checked" if fsl.mtf and fsl.gtf and fsl.rotary else "Empty",
            },
        )

        # MagneticToolFaceFrame
        mtf_element = ET.SubElement(fsl_element, "MagneticToolFaceFrame")

        # Populate MagneticToolFaceFrame based on dpoints
        if not fsl.mtf:
            mtf_element.text = " "
        else:
            for dpoint in fsl.mtf:
                ET.SubElement(mtf_element, "DataPoint", {"ID": str(dpoint.datpid)})

        # GravityToolFaceFrame
        gtf_element = ET.SubElement(fsl_element, "GravityToolFaceFrame")

        # Populate GravityToolFaceFrame based on dpoints
        if not fsl.gtf:
            gtf_element.text = " "
        else:
            for dpoint in fsl.gtf:
                ET.SubElement(gtf_element, "DataPoint", {"ID": str(dpoint.datpid)})

        # RotatingFrame
        rotary_element = ET.SubElement(fsl_element, "RotatingFrame")

        # Populate MagneticToolFaceFrame based on dpoints
        if not fsl.rotary:
            rotary_element.text = " "
        else:
            for dpoint in fsl.rotary:
                ET.SubElement(rotary_element, "DataPoint", {"ID": str(dpoint.datpid)})

    if frame_request.dds_frame:
        dds_element = ET.SubElement(
            run_element,
            "DDSFrame",
            {
                "Description": "DDS Frame",
                "BitRate": str(frame_request.fsl1.bitrate),
                "ROP": str(frame_request.fsl1.rop),
                "Notes": f"{frame_request.job_number}_{frame_request.well_name}_{frame_request.section_size}_{date}",
                "CheckStatus": "Checked",
            },
        )

        for dpoint in frame_request.dds_frame:
            ET.SubElement(dds_element, "DataPoint", {"ID": str(dpoint.datpid)})

    # UtilityFrame
    utility_element = ET.SubElement(
        run_element,
        "UtilityFrame",
        {
            "Description": "Utility Frame",
            "BitRate": str(frame_request.fsl1.bitrate),
            "ROP": str(frame_request.fsl1.rop),
            "Notes": f"{frame_request.job_number}_{frame_request.well_name}_{frame_request.section_size}_{date}",
            "CheckStatus": "Checked",
        },
    )
    if not frame_request.utility.utility:
        utility_element.text = " "
    else:
        for dpoint in frame_request.utility.utility:
            ET.SubElement(utility_element, "DataPoint", {"ID": str(dpoint.datpid)})

    if str(mwd_tool.tool_id) != "SPTOOL":
        # ODFFrame
        odf_element = ET.SubElement(
            run_element,
            "OnDemandFrame",
            {
                "Description": "ODF",
                "BitRate": str(frame_request.fsl1.bitrate),
                "ROP": str(frame_request.fsl1.rop),
                "Notes": f"{frame_request.job_number}_{frame_request.well_name}_{frame_request.section_size}_{date}",
                "CheckStatus": "Checked" if frame_request.odf_frame else "Empty",
            },
        )

        if not frame_request.odf_frame:
            odf_element.text = " "
        else:
            for dpoint in frame_request.odf_frame:
                ET.SubElement(odf_element, "DataPoint", {"ID": str(dpoint.datpid)})

        # MODFFrame
        if frame_request.modf_frame:
            modfset_element = ET.SubElement(
                run_element,
                "MODFSet",
                {
                    "SubName": "MODFSet 1",
                    "LWDLTBID": str(
                        next(
                            tool.ltb_addr
                            for tool in frame_request.tools
                            if tool.display_name in ["ARC6", "ECO"]
                        )
                    ),
                    "LockLevel": "Unlocked",
                },
            )

            for i, modf_set in enumerate(frame_request.modf_frame):
                modf_element = ET.SubElement(
                    modfset_element,
                    "MODF",
                    {
                        "Name": (
                            f"ECO APPO2 MODF{i + 1}"
                            if "ECO" in [tool.display_name for tool in frame_request.tools]
                            else f"ARC6 APPO MODF{i + 1}"
                        ),
                        "BitRate": str(frame_request.fsl1.bitrate),
                        "ROP": str(frame_request.fsl1.rop),
                        "Notes": f"{frame_request.job_number}_{frame_request.well_name}_{frame_request.section_size}_{date}",
                        "CheckStatus": "Checked",
                    },
                )

                # Add DataPoint elements to MODF
                if not modf_set:
                    modf_element.text = " "
                else:
                    for dpoint in modf_set:
                        ET.SubElement(modf_element, "DataPoint", {"ID": str(dpoint.datpid)})

    ET.SubElement(run_element, "CustomRuleFile", {"Name": "CustomRuleFile"})

    ET.SubElement(
        run_element,
        "QCElement",
        {
            "HSPMVersion": QC_ELEMENT_MAPPER.get(frame_request.hspm_version, "TnAShared2023_1_001"),
            "FileDateTime": str(datetime.now().isoformat()),
            "LocationSpecificRules": "",
            "QCCheck": "NoErrorOrWarning",
        },
    )

    # Save the XML to the specified path
    tree = ET.ElementTree(run_element)
    # tree.write(file_path, encoding="utf-8", xml_declaration=True)
    xml_string = ET.tostring(
        tree.getroot(), encoding="utf-8", method="xml", xml_declaration=True
    )

    return xml_string
