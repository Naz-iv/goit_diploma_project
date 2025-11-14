import json
import pytest
import azure.functions as func
from unittest.mock import MagicMock, patch

from main import request_frames as request_frames_func

@pytest.fixture
def mock_sql_handler():
    """Mock SQLHandler for database interactions"""
    with patch("function_app.SQLHandler") as mock:
        yield mock

@pytest.fixture
def mock_signer():
    """Mock DigitalSignature"""
    with patch("function_app.DigitalSignature") as mock:
        mock_instance = mock.return_value
        mock_instance.sign_doc.side_effect = lambda x: f"<signed>{x}</signed>"
        yield mock_instance

@pytest.fixture
def mock_main():
    """Mock main frame processing function"""
    with patch("function_app.main") as mock:
        yield mock

@pytest.fixture
def test_request():
    """Creates a valid request with UID"""
    return func.HttpRequest(
        method="POST",
        url="/api/request_frames",
        body=json.dumps({"uid": "12345", "hspm_version": "HSPM 2025.0"}).encode("utf-8"),
        headers={}
    )

### TESTS ###

def test_request_frames_existing_uid(mock_sql_handler, mock_signer, mock_main, test_request):
    """Test case where UID exists in DB with a value"""
    mock_sql_handler.return_value.__enter__.return_value.load_xml_from_sqlite.return_value = "<frame>data</frame>"

    response = request_frames_func(test_request)

    assert response.status_code == 200
    assert response.get_body().decode() == "<signed><frame>data</frame></signed>"

def test_request_frames_new_uid(mock_sql_handler, mock_signer, mock_main, test_request):
    """Test case where UID is new, processes frame generation"""
    mock_sql_handler.return_value.__enter__.return_value.load_xml_from_sqlite.return_value = None
    mock_main.return_value = "<frame>generated</frame>"

    response = request_frames_func(test_request)

    assert response.status_code == 200
    assert response.get_body().decode() == "<signed><frame>generated</frame></signed>"

def test_request_frames_no_uid():
    """Test case where UID is missing"""
    req = func.HttpRequest(
        method="POST",
        url="/api/request_frames",
        body=json.dumps({"hspm_version": "HSPM 2025.0"}).encode("utf-8"),
        headers={}
    )

    response = request_frames_func(req)


    assert response.status_code == 400
    assert response.get_body().decode() == "Invalid request body"

def test_request_frames_invalid_json():
    """Test case where request has invalid JSON"""
    req = func.HttpRequest(
        method="POST",
        url="/api/request_frames",
        body=b"{invalid_json}",
        headers={}
    )

    response = request_frames_func(req)

    assert response.status_code == 400
    assert response.get_body().decode() == "Invalid request body"

def test_request_frames_no_result_yet(mock_sql_handler, test_request):
    """Test case where UID exists but no result is available yet"""
    mock_sql_handler.return_value.__enter__.return_value.load_xml_from_sqlite.return_value = ""

    response = request_frames_func(test_request)

    assert response.status_code == 503
    assert response.get_body().decode() == "Frame Generator was not able to build frame for your request. Please consider increasing bit rate or reducing required update rates."

def test_request_frames_processing_fails(mock_sql_handler, mock_main, test_request):
    """Test case where frame generation fails"""
    mock_sql_handler.return_value.__enter__.return_value.load_xml_from_sqlite.return_value = None
    mock_main.return_value = None

    response = request_frames_func(test_request)

    assert response.status_code == 406
    assert "Unable to build frames" in response.get_body().decode()
