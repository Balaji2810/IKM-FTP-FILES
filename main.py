import logging
import schedule
import time

import get_ftp_files
import s3_upload
from config import FILE_STRUCTURE

logging.basicConfig(filename="log.txt", level=logging.DEBUG)


def job(filename):
    print("working on :", filename)
    f_name = get_ftp_files.download_file(filename)
    if f_name is not None:
        # s3_upload.upload_to_s3(file_name)
        return True
    return False


for filename in FILE_STRUCTURE:
    print("Scheduled ", filename)
    for s_time in FILE_STRUCTURE[filename]["time"]:
        schedule.every().monday.at(s_time).do(job, filename)
        schedule.every().tuesday.at(s_time).do(job, filename)
        schedule.every().wednesday.at(s_time).do(job, filename)
        schedule.every().thursday.at(s_time).do(job, filename)
        schedule.every().friday.at(s_time).do(job, filename)


while True:
    schedule.run_pending()
    time.sleep(60)
