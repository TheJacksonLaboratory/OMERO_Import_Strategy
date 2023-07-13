import errno
import logging
import os
import shutil
import sys
import time
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import mysql.connector
import pandas as pd
import utils
import requests
import urllib


class MonitorFolder(FileSystemEventHandler):
    def on_created(self, event):
        print(event.src_path, event.event_type)
        logger.info(event.src_path + " " + event.event_type)

        created_file = event.src_path

        if os.path.isdir(created_file):
            IMG_INFO = GET_IMAGE_INFO(FILE_TO_BE_IMPORTED=created_file)
            generate_submission_form(IMG_INFO=IMG_INFO,
                                     username=username,
                                     wkgroup=wkgroup,
                                     filename=created_file.split("/")[-1] + ".xlsx",
                                     PARENT_DIR=created_file)

    def on_modified(self, event):
        print(event.src_path, event.event_type)
        logger.info(event.src_path + " " + event.event_type)

    def on_deleted(self, event):
        print(event.src_path, event.event_type)
        logger.info(event.src_path + " " + event.event_type)

    def on_moved(self, event):
        print(event.src_path, event.event_type)
        logger.info(event.src_path + " " + event.event_type)


def authenticate() -> None:
    pass


def get_token() -> str:
    pass


def transfer_files() -> None:
    pass


def GET_IMAGE_INFO(FILE_TO_BE_IMPORTED: list[str]) -> pd.DataFrame:
    """
    Function to get metadata of an image from database
    :param: FILE_TO_BE_IMPORTED: Subfolders in the OMERO import folder,
            i.e. //bht2stor.jax.org/phenotype/OMERO/KOMP/ImagesToBeImportedIntoOmero
    :type FILE_TO_BE_IMPORTED: list[str]
    :return: Metadata of images
    :rtype: pd.DataFrame
    """

    logger.info("Connecting to database")
    conn = mysql.connector.connect(host=db_server, user=db_username, password=db_password, database=db_name)
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    stmt = """ SELECT
                    'KOMP' as 'Project',  -- Project
                    StockNumber AS 'dataset',
                    'KOMP_EYE',  -- OMERO_group
                    OrganismID,
                    MarkerSymbol AS `gene`,
                    GenotypeSymbol AS `genotype`,
                    Sex, 
                    DateBirth, -- DATEDIFF(DateOfTest, DateBirth) in days
                    ProcedureAlias, 
                    DateCompleteMap.DateComplete
                FROM
                    Organism
                        INNER JOIN
                    Line USING (_Line_key)
                        INNER JOIN
                    Genotype USING (_Organism_key)
                        INNER JOIN
                    cv_GenotypeSymbol USING (_GenotypeSymbol_key)
                        INNER JOIN
                    LineMarker USING (_Line_key)
                        INNER JOIN
                    Marker USING (_Marker_key)
                        INNER JOIN
                    cv_Sex USING (_Sex_key)
                        INNER JOIN
                    ProcedureInstanceOrganism USING (_Organism_key)
                        INNER JOIN
                    ProcedureInstance USING (_ProcedureInstance_key)
                        INNER JOIN
                    DateCompleteMap USING (_ProcedureInstance_key)
                WHERE 
                    ProcedureAlias = '{}'
                AND 
                    OrganismID = '{}';"""

    DB_RECORDS = []
    EYE_INFO = []
    for f in FILE_TO_BE_IMPORTED:

        # GET filenames from Omero import table
        logger.info(f"Process file {f}")
        organism_id = f.split("_")[1]

        def get_eye() -> str:
            """
            Function to get eye of the image from file name
            :return: Animal ID of the mouse
            :rtype: String
            """
            return f.split("_")[2].split(" ")[0]

        eye = utils.Eyes[get_eye()]

        def get_test() -> str:
            """
            Function to get experiment name from file name
            :return: Experiment name of the image
            :rtype: String
            """
            return f.split("_")[0]

        test = utils.TEST[get_test()]

        EYE_INFO.append(eye)
        logger.debug(f"Get metadata of image associated with animal {organism_id}")
        cursor.execute(stmt.format(test, organism_id))
        record = cursor.fetchall()

        if record:
            def to_lower_case(dict_: dict) -> dict:
                """
                :param dict_: Record of table in database
                :type dict_: dict
                :return: DB record with keys in all lower case
                :rtype: dict
                """
                if not dict_:
                    return {}

                return {k.lower(): v for k, v in dict_.items()}

            DB_RECORDS.append(to_lower_case(record[0]))
            # DB_RECORDS.append(record[0])

    cursor.close()
    conn.close()

    # print(DB_RECORDS)
    EYE_INFO = pd.DataFrame(EYE_INFO, columns=["Eye"])
    IMG_METADTA = pd.DataFrame(DB_RECORDS)
    IMG_FILE_NAME = pd.DataFrame({'filename': FILE_TO_BE_IMPORTED})
    # print(IMG_METADTA)
    # print(IMG_FILE_NAME)

    IMG_INFO = pd.concat([IMG_FILE_NAME, IMG_METADTA, EYE_INFO], axis=1)
    IMG_INFO = IMG_INFO.reset_index(drop=True)

    print("Resulting cells are:")
    print(IMG_INFO)

    return IMG_INFO


def generate_submission_form(IMG_INFO: pd.DataFrame,
                             username: str,
                             wkgroup: str,
                             filename: str,
                             PARENT_DIR: str) -> None:
    """
    Function to create the submission form for omero import
    :param IMG_INFO:Metadata to be inserted into excel spreadsheet
    :type IMG_INFO: pd.DataFrame
    :param username: Username of OMERO
    :type username: String
    :param wkgroup: Work group of OMERO
    :type wkgroup: String
    :param filename: Name of generated excel file
    :type filename: String
    :param PARENT_DIR: Directory to put the generated submission form
    :type PARENT_DIR: String
    :return: None
    :rtype:
    """
    credentials = {"OMERO user:": username, "OMERO group:": wkgroup}
    USER_INFO = pd.DataFrame.from_dict(credentials, orient="index")
    print(f"Crendentials is {USER_INFO}")
    print(USER_INFO)

    logger.debug(f"Generating form {filename}")
    with pd.ExcelWriter(filename,
                        mode='w') as writer:
        USER_INFO.to_excel(writer, sheet_name='Submission Form', startrow=0, startcol=0, header=False)
        IMG_INFO.to_excel(writer, sheet_name='Submission Form', startrow=4, startcol=0, header=True, index=False)

    def send_to(file: str, dest: str) -> None:
        """

        :param file:
        :type file:
        :param dest:
        :type dest:
        :return:
        :rtype:
        """
        try:
            logger.debug(f"Send {file} to {dest}")
            shutil.copy(file, dest)

        except FileExistsError as e:
            pass

    send_to(file=filename, dest=PARENT_DIR)


##########################################
def process(FROM: str,
            TO: str) -> None:
    """
    Function to organize the workflow of the script
    :return: None    :rtype:
    """
    FILE_TO_BE_IMPORT = os.listdir(FROM)
    print(FILE_TO_BE_IMPORT)

    for File in FILE_TO_BE_IMPORT:
        SUB_DIR = FROM + "/" + File
        if os.path.isdir(SUB_DIR):
            images = os.listdir(SUB_DIR)
            images_info = GET_IMAGE_INFO(FILE_TO_BE_IMPORTED=images)
            generate_submission_form(images_info,
                                     username=username,
                                     wkgroup=wkgroup,
                                     filename=File + ".xlsx",
                                     PARENT_DIR=SUB_DIR)

            logger.debug(f"Drop folder {SUB_DIR} to OMERO Dropbox")

        else:
            logger.error("Not a directory")
            pass


def main():
    src_path = os.getcwd()
    event_handler = MonitorFolder()
    observer = Observer()
    observer.schedule(event_handler, path=src_path, recursive=True)
    logger.info("Monitoring started")
    observer.start()
    try:
        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        observer.stop()
        observer.join()

    FROM = utils.FROM
    TO = utils.TO

    logger.info("Start importing to OMERO")
    # process(FROM=FROM, TO=TO)
    logger.info("Process finished")
    sys.exit()


if __name__ == "__main__":
    username = "chent"
    wkgroup = "default"
    submission_form = "OMERO_submission_form.xlsx"

    db_server = utils.db_server
    db_username = utils.db_username
    db_password = utils.db_password
    db_name = utils.db_name

    Eyes = {
        "OD": "Right eye",
        "OS": "Left Eye",
        "OU": "Both"
    }

    TEST = {
        "fundus2": "Eye Morphology",
        "path": "Gross Pathology",
        "fundus": "ERG"
    }

    """Setup logger"""

    job_name = 'OMERO_Import'
    logging_dest = os.path.join(os.getcwd(), "../logs")
    date = datetime.now().strftime("%B-%d-%Y")
    logging_filename = logging_dest + "/" + f'{date}.log'
    logger = utils.createLogHandler(logging_filename)
    logger.info('Logger has been created')

    main()
