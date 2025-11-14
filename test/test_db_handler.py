import pytest
import sqlite3
from unittest.mock import patch
from pathlib import Path
from utils.db_managment import SQLHandler 


@pytest.fixture
def in_memory_db():
    """Fixture to create an in-memory SQLite database."""
    with patch.object(SQLHandler, "__init__", lambda self, file_name=None: None):
        handler = SQLHandler()
        handler.conn = sqlite3.connect(":memory:")  # Use an in-memory database
        cursor = handler.conn.cursor()

        # Create test tables
        cursor.execute("""
            CREATE TABLE XmlStorage (
                uid TEXT PRIMARY KEY,
                xml_content TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE DPoint (
                DPOINT_NAME TEXT,
                DPOINT_DATPID INTEGER,
                NO_OF_BITS INTEGER,
                TOOL_LTB_ADDR INTEGER,
                DPOINT_SHORT_DESCRIPTION TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE Tool (
                ToolID TEXT PRIMARY KEY,
                DisplayToolName TEXT,
                LTBAddress INTEGER,
                IsMWDTool INTEGER,
                DPointNameSuffix TEXT
            )
        """)
        cursor.execute("""
            CREATE TABLE ToolVersion (
                ToolID TEXT,
                Version TEXT,
                FOREIGN KEY (ToolID) REFERENCES Tool (ToolID)
            )
        """)
        handler.conn.commit()
        yield handler  # Provide the handler to tests
        handler.conn.close()


def test_load_xml_from_sqlite_empty(in_memory_db):
    """Test that loading a non-existent UID returns None."""
    assert in_memory_db.load_xml_from_sqlite("nonexistent_uid") is None


def test_insert_uid_with_none(in_memory_db):
    """Test inserting a new UID with an empty XML content."""
    in_memory_db.insert_uid_with_none("test_uid")
    cursor = in_memory_db.conn.cursor()
    cursor.execute("SELECT xml_content FROM XmlStorage WHERE uid = ?", ("test_uid",))
    result = cursor.fetchone()
    assert result is not None
    assert result[0] == ""


def test_update_xml_in_sqlite(in_memory_db):
    """Test updating an XML entry."""
    in_memory_db.insert_uid_with_none("test_uid")
    in_memory_db.update_xml_in_sqlite("test_uid", "<frame>Test XML</frame>")
    
    cursor = in_memory_db.conn.cursor()
    cursor.execute("SELECT xml_content FROM XmlStorage WHERE uid = ?", ("test_uid",))
    result = cursor.fetchone()
    
    assert result is not None
    assert result[0] == "<frame>Test XML</frame>"


def test_select_dpoints_from_db_with_tools(in_memory_db):
    """Test selecting DPoints with associated tools."""
    cursor = in_memory_db.conn.cursor()
    cursor.execute("INSERT INTO Tool (ToolID, DisplayToolName, LTBAddress, IsMWDTool, DPointNameSuffix) VALUES ('tool1', 'TestTool', 100, 1, 'SUF')")
    cursor.execute("INSERT INTO DPoint (DPOINT_NAME, DPOINT_DATPID, NO_OF_BITS, TOOL_LTB_ADDR, DPOINT_SHORT_DESCRIPTION) VALUES ('DP1', 101, 16, 100, 'Test Desc')")
    in_memory_db.conn.commit()
    
    result = in_memory_db.select_dpoints_from_db_with_tools(["DP1"], [100])
    assert result is not None
    assert len(result) == 1
    assert result[0][0] == "DP1"


def test_select_tools_from_db(in_memory_db):
    """Test selecting tools from the database."""
    cursor = in_memory_db.conn.cursor()
    cursor.execute("INSERT INTO Tool (ToolID, DisplayToolName, LTBAddress, IsMWDTool, DPointNameSuffix) VALUES ('tool1', 'TestTool', 100, 1, 'SUF')")
    cursor.execute("INSERT INTO ToolVersion (ToolID, Version) VALUES ('tool1', 'v1.0')")
    in_memory_db.conn.commit()

    result = in_memory_db.select_tools_from_db({"TestTool"})
    assert result is not None
    assert len(result) == 1
    assert result[0][0] == "tool1"  


def test_select_tool_latest_version_from_db(in_memory_db):
    """Test retrieving the latest tool version."""
    cursor = in_memory_db.conn.cursor()
    cursor.execute("INSERT INTO ToolVersion (ToolID, Version) VALUES ('tool1', 'v1.0')")
    cursor.execute("INSERT INTO ToolVersion (ToolID, Version) VALUES ('tool1', 'v2.0')")
    in_memory_db.conn.commit()

    result = in_memory_db.select_tool_latest_version_from_db("tool1")
    assert result == "v2.0" 


def test_save_xml_to_sqlite(in_memory_db):
    """Test saving XML content."""
    in_memory_db.save_xml_to_sqlite("test_uid", "<frame>XML Data</frame>")

    cursor = in_memory_db.conn.cursor()
    cursor.execute("SELECT xml_content FROM XmlStorage WHERE uid = ?", ("test_uid",))
    result = cursor.fetchone()

    assert result is not None
    assert result[0] == "<frame>XML Data</frame>"


def test_remove_uid_from_db(in_memory_db):
    """Test removing a UID from the database."""
    in_memory_db.insert_uid_with_none("test_uid")
    in_memory_db.remove_uid_from_db("test_uid")

    cursor = in_memory_db.conn.cursor()
    cursor.execute("SELECT * FROM XmlStorage WHERE uid = ?", ("test_uid",))
    result = cursor.fetchone()
    
    assert result is None
