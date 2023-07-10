import errno
import logging
import os
import sys
from datetime import datetime
import requests
from requests import exceptions
import mysql.connector
import pandas as pd


def log_in(username: str, password: str) -> None:
    """
    Function to sign in to the OMERO
    Parameters
    ----------
    username :
    password :

    Returns
    -------

    """

    session = requests.Session()
    url = "https://omeroweb.jax.org/api/"
    response = session.get(url, verify=True)

    content = response.json()['data']
    forms = content[-1]

    base_url = forms['url:base']
    r = session.get(base_url)
    logger.debug(base_url)
    print(base_url)
    print(r.content)

    urls = r.json()
    servers_url = urls['url:servers']
    print(servers_url)

    """Get CSRF Token"""
    token_url = urls["url:token"]
    csrf = session.get(token_url).json()["data"]

    """List the servers available to connect to"""
    servers = session.get(servers_url).json()['data']
    servers = [s for s in servers if s['server'] == 'omero']
    if len(servers) < 1:
        raise Exception("Found no server called 'omero'")
    server = servers[0]

    """Log In To Omero"""
    login_url = urls['url:login']
    print(login_url)
    logger.debug(login_url)
    session.headers.update({'X-CSRFToken': csrf,
                            'Referer': login_url})
    payload = {
                'username': username,
                'password': password,
                'server': server['id']
               }
    r = session.post(login_url, data=payload)
    login_rsp = r.json()

    try:
        r.raise_for_status()

    except exceptions.HTTPError as e:
        logger.error(f"Error {e}")
        print("Error {}".format(e))
        raise
    assert login_rsp['success']


def GET_DOWNLOAD_URLS() -> list[str]:
    pass


def GET_IMAGE_INFO(FILE_TO_BE_IMPORTED: list[str]) -> pd.DataFrame:
    """
    Function to get metadata of an image from database
    :param: FILE_TO_BE_IMPORTED: Files in the OMERO import folder,
            i.e. //bht2stor.jax.org/phenotype/OMERO/KOMP/ImagesToBeImportedIntoOmero
    :type FILE_TO_BE_IMPORTED:
    :return: Metadata of images
    :rtype: pd.DataFrame
    """

    logger.info("Connecting to database")
    conn = mysql.connector.connect(host=db_server, user=db_username, password=db_password, database=db_name)
    cursor = conn.cursor(buffered=True, dictionary=True)
    cursor.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")
    stmt = """ SELECT
                    _ProcedureInstance_key AS 'Test Code', OrganismID AS Animal
                FROM
                    Organism
                INNER JOIN
                    ProcedureInstanceOrganism USING (_Organism_key)
                INNER JOIN
                    ProcedureInstance USING (_ProcedureInstance_key)
                WHERE
                    _ProcedureDefinitionVersion_key = 231 
                AND 
                OrganismID = '{}';"""

    DB_RECORDS = []
    for f in FILE_TO_BE_IMPORTED:
        logger.info(f"Process file {f}")
        organism_id = f.split("_")[1]

        logger.debug(f"Get metadata of image associated with animal {organism_id}")
        cursor.execute(stmt.format(organism_id))
        record = cursor.fetchall()

        if record:
            DB_RECORDS.append(record[0])
            #DB_RECORDS.append(record[0])

    cursor.close()
    conn.close()

    # print(DB_RECORDS)
    IMG_METADTA = pd.DataFrame(DB_RECORDS)
    IMG_FILE_NAME = pd.DataFrame({'filename': FILE_TO_BE_IMPORTED})
    # print(IMG_METADTA)
    # print(IMG_FILE_NAME)

    IMG_INFO = pd.concat([IMG_FILE_NAME, IMG_METADTA], axis=1)
    IMG_INFO = IMG_INFO.reset_index(drop=True)

    print("Resulting cells are:")
    print(IMG_INFO)

    return IMG_INFO


def main():
    FROM = "//bht2stor.jax.org/phenotype/OMERO/KOMP/ImagesToBeImportedIntoOmero"


if __name__ == "__main__":
    username = "chent"
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

    TEST = {
        "fundus2": "Eye Morphology",
        "path": "Gross Pathology",
        "fundus": "ERG"
    }

    #ProcedureDefinitionKey: Eye Morphology=231, Gross Pathology = 230, ERGv2=274

    PROC_DEF_KEY = {
        "Eye Morphology": 231,
        "Gross Pathology": 230,
        "ERGv2": 274
    }

    main()
