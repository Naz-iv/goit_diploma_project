import os
import json
import logging

from utils.db_managment import SQLHandler
from signature.digital_signature import DigitalSignature
from utils.utility import main, hspm_version
from hspm_version_mapper import HSPM_VERSION_MAP

from frame_builder import build_frames_from_json


def run_bulder_with_given_input(filepath: str):
    with open(filepath, "r") as f:
        input_data = json.load(f)
    
    frame_output = build_frames_from_json(input_data)

def run_bulder_with_given_data_json(filepath: str):
    with open(filepath, "r") as f:
        input_data = json.load(f)
    
    frame_output = main(input_data)

def function_app_mock(input_file: str):

    signer = DigitalSignature()

    logging.info("Parsing request data")

    # Request Body
    try:
        with open(input_file, "r") as f:
            data = json.load(f)
        uid = data.get("uid")

        hspm_version_from_user = HSPM_VERSION_MAP.get(data.get("hspm_version"))
        if hspm_version_from_user:
            hspm_version = hspm_version_from_user
        if not uid:
            raise ValueError("UID is missing in the request.")
    except Exception as e:
        logging.error(f"Invalid request body: {e}")
        return "Invalid request body"

    logging.info("Request data parsed OK")

    # Handling database access with concurrent support
    try:
        with SQLHandler(hspm_version) as db:
            logging.info(f"Inserting UID {uid} with initial value None.")
            db.insert_uid_with_none(uid)

            logging.info("Write UID to DB OK")

        # Process the request
        xml_string, status = main(data)

        logging.info("Frames proceesed OK")

        if xml_string:
            signer = DigitalSignature()

            signed_xml = signer.sign_doc(xml_string)
            with open("inhouse_test_result.fbw", "w") as output_file:
                output_file.write(signed_xml)

            with SQLHandler(hspm_version) as db:
                logging.info(f"Updating DB with result for UID {uid}.")
                db.update_xml_in_sqlite(uid, xml_string, status)

            logging.info("Frames sent to user")

            logging.info(f"Frame request processed successfully for UID {uid}!")
            return "Frame generated and writen to DB"
        else:
            error_message = "Unable to build frames for your request. Please check compatibility!"
            logging.info(f"Frame request failed for UID {uid}, marking as FAILED in DB.")

            with SQLHandler(hspm_version) as db:
                db.update_xml_in_sqlite(uid, f"FAILED | {error_message}")

            return error_message
    except Exception as e:
        error_message = str(e)
        logging.error(f"Error occurred: {type(e)} - {e}")

        if "the JSON object must be str, bytes or bytearray, not NoneType" in error_message:
            error_message = "RTOF.json file didn't load correctly. Please check your input and re-submit request."

        with SQLHandler(hspm_version) as db:
            db.update_xml_in_sqlite(uid, f"FAILED | {error_message}")

        return error_message

def clear_db_on_failure(hstm_version, uid):
    # Remove UID from DB to allow new request for frame generation
    with SQLHandler(hspm_version) as db:
        logging.info(f"Clearign UID form DB for UID {uid}.")
        db.remove_uid_from_db(uid)

if __name__ == "__main__":
    # test_framebuilder(r"C:\Users\nivankiv\OneDrive - SLB\Promotions\G11\FrameGenerator InhouseTesting")
    
    function_app_mock("O.1074848.01_GCTM3_8.5_2025-11-13_08-48request_data.json")