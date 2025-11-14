import pyodbc
import os
import logging
import sqlite3
import time

from pathlib import Path
from typing import List, Optional, Set, Tuple


class MDBHandler:
    """
    A handler for managing and interacting with a Microsoft Access Database (MDB).

    The MDBHandler class provides methods to open, close, and perform specific
    database operations on an MDB file. It is designed for working with tools and
    DPoints data while leveraging the pyodbc library for database connections.
    The class supports context manager functionality for seamless resource management.

    Attributes:
        db_conn (pyodbc.Connection or None): The database connection object used
        for executing queries.
        db_path (str): Absolute path to the MDB file being accessed.
        tool_synonyms (list of list): A predefined list of tool synonyms for internal operations.
    """
    def __init__(self, file_name: str = "RTDPointCatalog_2025_0.mdb"):
        self.db_conn = None
        db_path = os.path.join(Path(__file__).resolve().parent, file_name)
        self.db_path = db_path
        self.tool_synonyms = [["GVR4", "MICROSCOPE"]]

    def __enter__(self):
        self.db_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_close()

    def db_open(self) -> bool:
        try:
            conn_str = (
                r"DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};"
                f"DBQ={self.db_path};"
                "PWD=shruti;"
            )
            self.db_conn = pyodbc.connect(conn_str)
            return True
        except pyodbc.Error as e:
            logging.error("Error opening database:", e)
            return False

    def db_close(self):
        if self.db_conn:
            self.db_conn.close()

    def select_dpoints_from_db_with_tools(
        self, dpoint_names: List[str], tools: List[int]
    ) -> Optional[tuple]:

        dpoints_placeholder = ", ".join(["?"] * len(dpoint_names))
        tools_placeholder = ", ".join(["?"] * len(tools))

        query = f"""
            SELECT DPoint.DPOINT_NAME, DPoint.DPOINT_DATPID, DPoint.NO_OF_BITS, Tool.IsMWDTool, DPoint.TOOL_LTB_ADDR, DPoint.DPOINT_SHORT_DESCRIPTION 
            FROM DPoint
            INNER JOIN Tool ON Tool.LTBAddress = DPoint.TOOL_LTB_ADDR
            WHERE DPoint.TOOL_LTB_ADDR IN ({tools_placeholder}) AND DPoint.DPOINT_NAME IN ({dpoints_placeholder})
            ORDER BY DPoint.DPOINT_NAME ASC;
        """

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, *tools, *dpoint_names)
            result = cursor.fetchall()
            return result
        except pyodbc.Error as e:
            logging.info("Error retrieving DPoints for tool:", e)
            return None

    def select_tools_from_db(self, tool_names: Set[str]) -> Optional[List[tuple]]:
        # Define the columns you want to select
        columns = "Tool.ToolID, Tool.DisplayToolName, Tool.LTBAddress, Tool.IsMWDTool, Tool.DPointNameSuffix, MAX(ToolVersion.Version) AS Version"

        # Create a parameterized WHERE clause for a list of tool names
        placeholders = ",".join(["?"] * len(tool_names))
        query = f"""
        SELECT {columns} 
        FROM Tool 
        Left JOIN ToolVersion ON ToolVersion.ToolID = Tool.ToolID 
        WHERE Tool.DisplayToolName IN ({placeholders})
        GROUP BY Tool.ToolID, Tool.DisplayToolName, Tool.LTBAddress, Tool.IsMWDTool, Tool.DPointNameSuffix;
        """

        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, tool_names)
            result = cursor.fetchall()
            return result
        except pyodbc.Error as e:
            logging.info("Error retrieving tools data:", e)
            return None

    def select_tool_latest_version_from_db(self, tool_id: str) -> Optional[str]:
        query = """
            SELECT Version FROM ToolVersion
            WHERE ToolID = ?
            ORDER BY Version DESC;
        """
        try:
            cursor = self.db_conn.cursor()
            cursor.execute(query, (tool_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except pyodbc.Error as e:
            logging.info("Error retrieving latest tool version:", e)
            return None


class SQLHandler:
    """
    Handles operations with an SQLite database, managing connection and executing common SQL
    queries and operations. It provides an interface for storing, retrieving, and managing data
    related to XML content, data points, tools, and their relationships.

    The class uses a context manager to ensure the proper opening and closing of the database
    connection. It employs a retry mechanism to handle potential database locks and supports
    a variety of operations, including CRUD functionalities, ensuring streamlined interaction
    with the SQLite database.

    Attributes:
    db_path: str
        Absolute path to the SQLite database file.
    conn: Optional[sqlite3.Connection]
        Connection object to the SQLite database, initialized when the connection is opened.

    Methods Section:
    __enter__():
        Context manager entry point to open the database connection.
    __exit__():
        Context manager exit point to close the database connection.
    retry_on_lock():
        Decorator to retry a function if the database is locked.
    db_open():
        Opens a connection to the SQLite database.
    db_close():
        Closes the SQLite database connection safely.
    load_xml_from_sqlite(uid: str) -> Optional[str]:
        Retrieves XML content associated with a given UID.
    insert_uid_with_none(uid: str):
        Inserts a UID with an empty content value into the database.
    update_xml_in_sqlite(uid: str, xml_content: str):
        Updates existing XML content in the database for the specified UID.
    select_dpoints_from_db_with_tools(dpoint_names: List[str], tools: List[int]) -> Optional[List[tuple]]:
        Fetches data points based on given names and tools.
    select_tools_from_db(tool_names: Set[str]) -> Optional[List[tuple]]:
        Retrieves tool data based on specified tool names.
    select_tool_latest_version_from_db(tool_id: str) -> Optional[str]:
        Gets the latest version of a tool based on its Tool ID.
    save_xml_to_sqlite(uid: str, xml_content: str):
        Saves XML content to the SQLite database under the corresponding UID.
    remove_uid_from_db(uid: str):
        Removes an entry from the database based on the provided UID.
    """
    def __init__(self, file_name: str = "RTDPointCatalog_2025_0.db"):
        self.db_path = os.path.join(Path(__file__).resolve().parent, file_name)
        # Testing access to Azure files storage to get write access to file
        # self.db_path = os.path.join("/mnt/storage", file_name)
        self.conn = None

    def __enter__(self):
        self.db_open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.db_close()

    def db_open(self) -> bool:
        """Open a connection to the SQLite database."""
        try:
            self.conn = sqlite3.connect(
                self.db_path, timeout=15
            )  # Set a timeout for connections
            return True
        except sqlite3.Error as e:
            logging.error(f"Error connecting to SQLite database: {e}")
            return False

    def db_close(self):
        """Close the connection to the SQLite database."""
        if self.conn:
            self.conn.close()

    def retry_on_lock(func):
        """Decorator to retry a function when a database is locked."""

        def wrapper(*args, **kwargs):
            retries = 5
            for attempt in range(retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() and attempt < retries - 1:
                        time.sleep(
                            0.5 * (attempt + 1)
                        )  # Exponentially increase sleep time
                    else:
                        raise

        return wrapper

    @retry_on_lock
    def load_xml_from_sqlite(self, uid: str) -> Optional[Tuple[str, str]]:
        """Load XML content and status from the SQLite database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT xml_content, status FROM XmlStorage WHERE uid = ?", (uid,))
            result = cursor.fetchone()
            if result:
                return result  # Tuple (xml_content, status)
            else:
                return None
        except sqlite3.Error as e:
            logging.error(f"Error loading XML content from SQLite: {e}")
            return None

    @retry_on_lock
    def insert_uid_with_none(self, uid: str):
        """Insert UID with empty XML content and default status."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO XmlStorage (uid, xml_content, status) VALUES (?, ?, ?)",
                (uid, "", 200),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Error inserting UID {uid} into SQLite: {e}")

    @retry_on_lock
    def update_xml_in_sqlite(self, uid: str, xml_content: str, status: int = 200):
        """Update XML content and status for an existing UID in the SQLite database."""
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE XmlStorage SET xml_content = ?, status = ? WHERE uid = ?",
                    (xml_content, status, uid),
                )
        except sqlite3.Error as e:
            logging.error(f"Error updating XML content for UID {uid}: {e}")

    @retry_on_lock
    def select_dpoints_from_db_with_tools(
        self, dpoint_names: List[str], tools: List[int]
    ) -> Optional[List[tuple]]:
        """Select data points from the database based on tool and dpoint names."""
        dpoints_placeholder = ", ".join(["?"] * len(dpoint_names))
        tools_placeholder = ", ".join(["?"] * len(tools))

        query = f"""
            SELECT DPoint.DPOINT_NAME, DPoint.DPOINT_DATPID, DPoint.NO_OF_BITS, Tool.IsMWDTool, DPoint.TOOL_LTB_ADDR, DPoint.DPOINT_SHORT_DESCRIPTION
            FROM DPoint
            INNER JOIN Tool ON Tool.LTBAddress = DPoint.TOOL_LTB_ADDR
            WHERE DPoint.TOOL_LTB_ADDR IN ({tools_placeholder}) AND DPoint.DPOINT_NAME IN ({dpoints_placeholder})
            ORDER BY DPoint.DPOINT_NAME ASC;
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, tools + dpoint_names)
            result = cursor.fetchall()
            return result
        except sqlite3.Error as e:
            logging.error(f"Error retrieving DPoints for tools from SQLite: {e}")
            return None

    @retry_on_lock
    def select_tools_from_db(self, tool_names: Set[str]) -> Optional[List[tuple]]:
        """Select tool data based on the tool names."""
        placeholders = ", ".join(["?"] * len(tool_names))
        query = f"""
            SELECT Tool.ToolID, Tool.DisplayToolName, Tool.LTBAddress, Tool.IsMWDTool, Tool.DPointNameSuffix, MAX(ToolVersion.Version) AS Version
            FROM Tool
            LEFT JOIN ToolVersion ON ToolVersion.ToolID = Tool.ToolID
            WHERE Tool.DisplayToolName IN ({placeholders})
            GROUP BY Tool.ToolID, Tool.DisplayToolName, Tool.LTBAddress, Tool.IsMWDTool, Tool.DPointNameSuffix;
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, list(tool_names))
            result = cursor.fetchall()
            return result
        except sqlite3.Error as e:
            logging.error(f"Error retrieving tools data from SQLite: {e}")
            return None

    @retry_on_lock
    def select_tool_latest_version_from_db(self, tool_id: str) -> Optional[str]:
        """Select the latest version of a tool from the database."""
        query = """
            SELECT Version FROM ToolVersion
            WHERE ToolID = ?
            ORDER BY Version DESC;
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, (tool_id,))
            result = cursor.fetchone()
            return result[0] if result else None
        except sqlite3.Error as e:
            logging.error(f"Error retrieving latest tool version from SQLite: {e}")
            return None

    @retry_on_lock
    def save_xml_to_sqlite(self, uid: str, xml_content: str, status: int):
        """Save XML content and status to the SQLite database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "REPLACE INTO XmlStorage (uid, xml_content, status) VALUES (?, ?, ?)",
                (uid, xml_content, status),
            )
            self.conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Error saving XML content to SQLite: {e}")

    @retry_on_lock
    def remove_uid_from_db(self, uid: str):
        """Remove a specific UID entry from the SQLite database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM XmlStorage WHERE uid = ?", (uid,))
            self.conn.commit()
            logging.info(f"Successfully removed UID {uid} from the database.")
        except sqlite3.Error as e:
            logging.error(f"Error removing UID {uid} from SQLite: {e}")
