from glue.src.bronze.upload_working_file import upload_file
import datetime

yesterday = datetime.date.today() - datetime.timedelta(1)
year = yesterday.strftime('%Y')
month = yesterday.strftime('%m')
day = yesterday.strftime('%d')

def lambda_handler(event,context):
    upload_file("inventory.csv",f"erp",f"raw/year={year}/month={month}/day={day}/inventory.csv")

