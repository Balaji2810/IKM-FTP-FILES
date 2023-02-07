import logging
import schedule
import time
from datetime import datetime

import cloud.get_ftp_files as get_ftp_files
import cloud.s3_upload as s3_upload
from config import FILE_STRUCTURE

logging.basicConfig(
    filename="log.txt",
    format="%(asctime)s %(levelname)-8s %(message)s",
    level=logging.INFO,
    datefmt="%Y-%m-%d %H:%M:%S",
)


def job(filename):
    print("working on :", filename, datetime.utcnow())
    f_name = get_ftp_files.download_file(filename)
    if f_name is not None:
        print(f"{filename} downloaded--|")
        logging.info(f"{filename} downloaded--|")
        s3_upload.upload_to_s3(filename)
        print(f"{filename} uploaded to S3--|")
        logging.info(f"{filename} uploaded to S3--|")
        return True
    return False


for filename in FILE_STRUCTURE:
    for s_time in FILE_STRUCTURE[filename]["time"]:
        print(f"Scheduled {filename} everyday at {s_time}")
        logging.info(f"Scheduled {filename} everyday at {s_time}")
        schedule.every().day.at(s_time).do(job, filename)

        # if only for week days use the below code

        # schedule.every().monday.at(s_time).do(job, filename)
        # print(f"Scheduled {filename} monday at {s_time}")
        # logging.info(f"Scheduled {filename} monday at {s_time}")

        # schedule.every().tuesday.at(s_time).do(job, filename)
        # print(f"Scheduled {filename} tuesday at {s_time}")
        # logging.info(f"Scheduled {filename} tuesday at {s_time}")

        # schedule.every().wednesday.at(s_time).do(job, filename)
        # print(f"Scheduled {filename} wednesday at {s_time}")
        # logging.info(f"Scheduled {filename} wednesday at {s_time}")

        # schedule.every().thursday.at(s_time).do(job, filename)
        # print(f"Scheduled {filename} thursday at {s_time}")
        # logging.info(f"Scheduled {filename} thursday at {s_time}")

        # schedule.every().friday.at(s_time).do(job, filename)
        # print(f"Scheduled {filename} friday at {s_time}")
        # logging.info(f"Scheduled {filename} friday at {s_time}")


while True:
    schedule.run_pending()
    time.sleep(60)
