import logging
import os
import shutil
import sys
from datetime import datetime

import mysql.connector
import pandas as pd


def GET_IMAGE_INFO(FILE_TO_BE_IMPORTED: list[str]):
    """

    :param FILE_TO_BE_IMPORTED:
    :type FILE_TO_BE_IMPORTED:
    :return:
    :rtype:
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
                    ProcedureAlias = 'Eye Morphology'
                AND 
                    OrganismID = '{}';"""

    DB_RECORDS = []
    EYE_INFO = []
    for f in FILE_TO_BE_IMPORTED:
        logger.info(f"Process file {f}")
        organism_id = f.split("_")[1]

        def get_eye():
            return f.split("_")[2].split(" ")[0]

        eye = Eyes[get_eye()]
        EYE_INFO.append(eye)
        logger.debug(f"Get metadata of image associated with animal {organism_id}")
        cursor.execute(stmt.format(organism_id))
        record = cursor.fetchall()
        if record: DB_RECORDS.append(record[0])

    cursor.close()
    conn.close()

    # print(DB_RECORDS)
    EYE_INFO = pd.DataFrame(EYE_INFO)
    IMG_METADTA = pd.DataFrame(DB_RECORDS)
    IMG_FILE_NAME = pd.DataFrame({'Filename': FILE_TO_BE_IMPORTED})
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

    :param IMG_INFO:
    :type IMG_INFO:
    :param username:
    :type username:
    :param wkgroup:
    :type wkgroup:
    :param filename:
    :type filename:
    :param PARENT_DIR:
    :type PARENT_DIR:
    :return:
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
            shutil.copy(SUB_DIR, TO)

        else:
            logger.error("Not a directory")
            pass


def main():
    FROM = "//bht2stor.jax.org/phenotype/OMERO/KOMP/ImagesToBeImportedIntoOmero"
    TO = "//jax.org/jax/omero-drop/dropbox"
    logger.info("Start importing to OMERO")
    process(FROM=FROM, TO=TO)
    logger.info("Process finished")
    sys.exit()


if __name__ == "__main__":
    username = "xxxxx"
    wkgroup = "default"
    submission_form = "OMERO_submission_form.xlsx"

    db_server = "rslims.jax.org"
    db_username = "dba"
    db_password = "rsdba"
    db_name = "rslims"

    """Setup logger"""


    def createLogHandler(log_file):
        logger = logging.getLogger(__name__)
        FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
        logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter(FORMAT))
        logger.addHandler(handler)

        return logger


    job_name = 'OMERO_Import'
    logging_dest = os.path.join(os.getcwd(), "logs")
    date = datetime.now().strftime("%B-%d-%Y")
    logging_filename = logging_dest + "/" + f'{date}.log'
    logger = createLogHandler(logging_filename)
    logger.info('Logger has been created')

    Eyes = {
        "OD": "Right eye",
        "OS": "Left Eye",
        "OU": "Both"
    }

    main()
