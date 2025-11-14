from typing import List, Set
from dataclasses import dataclass, field


@dataclass()
class DPOINT:
    """
    Represents a data point with multiple attributes for measurement and processing.

    The DPOINT class encapsulates attributes related to a specific data point,
    configurable through initialization. It ensures that specific attributes are
    of appropriate types by typecasting during initialization. It also provides
    custom equality and hashing mechanisms, focusing on certain key fields.

    Attributes:
        name (str): The name of the data point.
        datpid (int): An identifier for the data point. Converts from string to
            int if provided as a string.
        length (int): The length value of the data point. Converts from string to
            int if provided as a string.
        is_mwd (bool): A flag representing if the data point is MWD (Measurement
            While Drilling). Converts from string to bool if provided as a string.
        ltb_addr (int): The address of the corresponding point in the LTB system.
            Converts from string to int if provided as a string.
        description (str): A description of the data point.
        time (int, optional): A time value associated with the data point. Defaults to None.
        depth (int, optional): A depth value associated with the data point.
            Defaults to None.

    """
    name: str
    datpid: int
    length: int
    is_mwd: bool
    ltb_addr: int
    description: str
    time: int = None
    depth: int = None

    def __post_init__(self):
        if isinstance(self.datpid, str):
            self.datpid = int(self.datpid)
        if isinstance(self.length, str):
            self.length = int(self.length)
        if isinstance(self.is_mwd, str):
            self.is_mwd = bool(int(self.is_mwd))
        if isinstance(self.ltb_addr, str):
            self.ltb_addr = int(self.ltb_addr)


    def __eq__(self, other):
        if not isinstance(other, DPOINT):
            return False
        # Custom equality: compare only certain fields
        return (
            self.name, self.datpid, self.ltb_addr, self.is_mwd, self.length
            ) == (
                other.name, other.datpid, other.ltb_addr, other.is_mwd, other.length
                )

    def __hash__(self):
        # Custom hash: based on specific fields
        return hash((
            self.name, self.datpid, self.ltb_addr, self.is_mwd, self.length
            ))


@dataclass
class TOOL:
    """
    Represents a tool with specific properties and configurations.

    This class is used for managing tool objects with attributes describing
    their identification, display specifications, settings, and additional
    parameters. It includes functionality for post-initialization processing,
    as well as custom equality and hashing implementations for tool objects.

    Attributes:
        tool_id: A unique identifier for the tool, typically a string.
        display_name: The name used for displaying the tool in user interfaces.
        ltb_addr: The logical tool bus address of the tool, used for communication.
        is_mwd: Indicates whether the tool is Measurement While Drilling (MWD)
            or not, stored as a boolean.
        dpoint_suffix: The suffix used for data points associated with the tool.
        version: The version identifier or code of the tool.
        size: An optional attribute specifying the size of the tool.
        tr_spacing: An optional attribute specifying the tool's spacing as a
            floating-point value.
        rt_blocksize: An optional integer specifying the block size used for
            real-time operations.

    Raises:
        ValueError: Raised during post-initialization if specific type
            conversions cannot be performed.

    """
    tool_id: str
    display_name: str
    ltb_addr: str
    is_mwd: bool
    dpoint_suffix: str
    version: str
    size: str = None
    tr_spacing: float = None
    rt_blocksize: int = None
    
    def __post_init__(self):
        if isinstance(self.is_mwd, str):
            self.is_mwd = bool(int(self.is_mwd))
        if isinstance(self.ltb_addr, str):
            self.ltb_addr = int(self.ltb_addr)
        
    def __eq__(self, other):
        if not isinstance(other, TOOL):
            return False
        # Custom equality: compare only certain fields
        return (
            self.tool_id, self.display_name, self.ltb_addr, self.is_mwd, self.dpoint_suffix
            ) == (
                other.tool_id, other.display_name, other.ltb_addr, other.is_mwd, other.dpoint_suffix
                )

    def __hash__(self):
        # Custom hash: based on specific fields
        return hash((
            self.tool_id, self.display_name, self.ltb_addr, self.is_mwd, self.dpoint_suffix
            ))


@dataclass
class FSL:
    """
    Represents the FSL configuration and its associated properties.

    This class is designed to manage and store the configuration details of an FSL system,
    including parameters such as bitrate, update intervals, rotary points, and space block values.
    The class uses type hints and default values for easy initialization and ensures that
    parameters with incorrect types are converted appropriately during post-initialization.

    Attributes:
        description (str): A textual description of the FSL system.
        bitrate (int): Bitrate configuration for the FSL system.
        rop (int): Rate of operation (ROP) setting for the system.
        nonorion_update (int): Frequency of updates for non-orion configurations, specified in units.
        orion_update (int): Frequency of updates for orion configurations, specified in units.
        R1_block (int): Block value for R1 configuration.
        R1_space (float): Spacing associated with the R1 configuration.
        R2_block (int): Block value for R2 configuration.
        R2_space (float): Spacing associated with the R2 configuration.
        R3_block (int): Block value for R3 configuration.
        R3_space (float): Spacing associated with the R3 configuration.
        mtf (List[DPOINT]): Multi-Time Format (MTF) points associated with the system.
        gtf (List[DPOINT]): General Time Format (GTF) points associated with the system.
        rotary (List[DPOINT]): Rotary points related to the system configuration.
    """
    description: str = ""
    bitrate: int = 6
    rop: int = 100
    nonorion_update: int = 1
    orion_update: int = 6
    R1_block: int = None
    R1_space: float = None
    R2_block: int = None
    R2_space: float = None
    R3_block: int = None
    R3_space: float = None
    min_bounds_pct: int = 10
    max_bounds_pct: int = 50
    max_dpoints: int = 165
    mtf: List[DPOINT] = field(default_factory=list)
    gtf: List[DPOINT] = field(default_factory=list)
    rotary: List[DPOINT] = field(default_factory=list)
    
    def __post_init__(self):
        if isinstance(self.rop, str):
            self.rop = float(self.rop)
        if isinstance(self.bitrate, str):
            self.bitrate = float(self.bitrate)
        if isinstance(self.nonorion_update, str):
            self.nonorion_update = float(self.nonorion_update)
        if isinstance(self.orion_update, str):
            self.orion_update = float(self.orion_update)

        
@dataclass
class UTILITY:
    """
    Represents a utility configuration with metadata details and a collection of points.

    This dataclass defines attributes for a utility configuration including a textual
    description, bitrate, rate of production, and a list of associated data points.
    It also ensures that specific attributes are converted into proper types during
    initialization.

    Attributes:
        description (str): A textual description of the utility.
        bitrate (int): The bitrate value associated with the utility.
        rop (int): The rate of production or another relevant integer parameter.
        utility (List[DPOINT]): A list of data points associated with the utility.

    Methods:
        __post_init__: Converts 'rop' and 'bitrate' to integers if their initial
        input is of type string.
    """
    description: str = ""
    bitrate: int = 6
    rop: int = 100
    utility: List[DPOINT] = field(default_factory=list)
    
    def __post_init__(self):
        if isinstance(self.rop, str):
            self.rop = int(self.rop)
        if isinstance(self.bitrate, str):
            self.bitrate = int(self.bitrate)
    

@dataclass
class FRAME_REQUEST:
    """
    FRAME_REQUEST represents a data structure designed to encapsulate information related to
    frame requests in an application. This structure includes multiple fields that represent
    various parameters of a frame request, such as well details, file selection details, utility,
    and other configuration-specific attributes. It is designed to manage and transfer data for
    frame operations, ensuring all necessary details are maintained as part of the object.

    Attributes:
        uid (str): Unique identifier for the frame request.
        job_number (str): The job number associated with the frame request.
        well_name (str): The name of the well linked to the frame request.
        section_size (str): Size of the section under consideration.
        fsl1 (FSL): First file selection list related to the request.
        fsl2 (FSL): Second file selection list related to the request.
        fsl3 (FSL): Third file selection list related to the request.
        fsl4 (FSL): Fourth file selection list related to the request.
        fsl5 (FSL): Fifth file selection list related to the request.
        fsl6 (FSL): Sixth file selection list related to the request.
        utility (UTILITY): Utility instance defining additional configuration tasks.
        num_of_fsl (int): Number of file selection lists being used.
        odf_required (bool): Defines if the ODF is required for this frame request.
        modf_required (bool): Indicates if the MODF is required for this request.
        provision_bha (bool): Specifies if BHA provision needs to be addressed.
        ddr_bha (bool): States whether DDR BHA is relevant for this frame.
        hspm_version (str): Version information of the HSPM catalog being used.
        tools (Set[TOOL]): Set of tools associated with this frame request.
        odf_frame (List[DPOINT]): ODF frame data points.
        modf_frame (List[List[DPOINT]]): MODF frame data arranged in nested lists of data points.
        dds_frame (List[DPOINT]): DDS frame data points.
    """
    uid: str
    job_number: str
    well_name: str
    section_size: str
    fsl1: FSL
    fsl2: FSL
    fsl3: FSL
    fsl4: FSL
    fsl5: FSL
    fsl6: FSL
    utility: UTILITY
    num_of_fsl: int = 1
    odf_required: bool = False
    modf_required: bool = False
    provision_bha: bool = False
    ddr_bha: bool = False
    hspm_version: str = "RTDPointCatalog_2025_0.db"
    tools: Set[TOOL] = field(default_factory=set)
    odf_frame: List[DPOINT] = field(default_factory=list)
    modf_frame: List[List[DPOINT]] = field(default_factory=list)
    dds_frame: List[DPOINT] = field(default_factory=list)
    