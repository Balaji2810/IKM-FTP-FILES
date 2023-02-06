import gzip
from datetime import datetime
from ftplib import FTP
from io import BytesIO
import logging
import configparser
from datetime import datetime

from config import FILE_STRUCTURE


config = configparser.ConfigParser()
config.read("secret.ini")


def get_ftp_connection() -> FTP:
    ftp_connection = FTP(config["IKM"]["ftp_host"])
    ftp_connection.login(config["IKM"]["ftp_username"], config["IKM"]["ftp_password"])
    return ftp_connection


def download_file(file_name):
    ftp_connection = get_ftp_connection()
    save_path = config["LOCAL"]["path"] + "/" + file_name
    date_str = FILE_STRUCTURE[file_name]["date_str"]
    compressed = FILE_STRUCTURE[file_name]["compressed"]
    if date_str:
        path = FILE_STRUCTURE[file_name]["path"].format(
            datetime.now().strftime("%d%m%Y")
        )
    else:
        path = FILE_STRUCTURE[file_name]["path"]

    try:
        if compressed:
            data = BytesIO()
            ftp_connection.retrbinary("RETR " + path, data.write)
            data.seek(0)
            uncompressed = gzip.decompress(data.read())
            with open(save_path, "wb") as file:
                file.write(uncompressed)
        else:
            with open(save_path, "wb") as f:
                ftp_connection.retrbinary("RETR " + path, f.write)
        logging.debug("downloaded %s %s file", path, FILE_STRUCTURE[file_name]["name"])
    except Exception as exc:
        logging.exception(
            "could not download %s file %s",
            FILE_STRUCTURE[file_name]["name"],
            file_name,
        )
        return
    else:
        return file_name
