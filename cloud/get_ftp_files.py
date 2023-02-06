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


def download_file(filename):
    ftp_connection = get_ftp_connection()
    save_path = config["LOCAL"]["path"] + "/" + filename
    date_str = FILE_STRUCTURE[filename]["date_str"]
    compressed = FILE_STRUCTURE[filename]["compressed"]
    if date_str:
        path = FILE_STRUCTURE[filename]["path"].format(
            datetime.now().strftime("%d%m%Y")
        )
    else:
        path = FILE_STRUCTURE[filename]["path"]

    try:
        logging.debug(
            "downloading %s %s file\n", path, FILE_STRUCTURE[filename]["name"]
        )
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
        logging.debug("downloaded %s %s file\n", path, FILE_STRUCTURE[filename]["name"])
    except Exception as exc:
        logging.error(
            "could not download %s file %s",
            FILE_STRUCTURE[filename]["name"],
            filename,
        )
        print(f"could not download {FILE_STRUCTURE[filename]['name']} file {filename}")
        return
    else:
        return filename
