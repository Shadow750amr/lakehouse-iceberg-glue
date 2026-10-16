import datetime
from zoneinfo import ZoneInfo

def get_time_zone(timezone:str = "America/Mexico_City") -> dict:
    ''' So basically this bring the t-1 local timezone using zoneinfo and saves it in a dict. By default brings the Mexico city timezone.'''
    local_tz = ZoneInfo(timezone)
    today = datetime.datetime.now(local_tz).date()
    yesterday = today - datetime.timedelta(days=1)
    return {"year": yesterday.strftime('%Y'),"month": yesterday.strftime('%m'), "day": yesterday.strftime('%d')}

