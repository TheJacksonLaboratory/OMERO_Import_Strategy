import logging


def createLogHandler(log_file):
    logger = logging.getLogger(__name__)
    FORMAT = "[%(asctime)s->%(filename)s->%(funcName)s():%(lineno)s]%(levelname)s: %(message)s"
    logging.basicConfig(format=FORMAT, filemode="w", level=logging.DEBUG, force=True)
    handler = logging.FileHandler(log_file)
    handler.setFormatter(logging.Formatter(FORMAT))
    logger.addHandler(handler)

    return logger


db_server = "rslims.jax.org"
db_username = "dba"
db_password = "rsdba"
db_name = "rslims"

FROM = "//bht2stor.jax.org/phenotype/OMERO/KOMP/ImagesToBeImportedIntoOmero"
TO = "//jax.org/jax/omero-drop/dropbox"

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
