import time
from datetime import datetime

from dateutil.relativedelta import relativedelta
from pytz import timezone


class datetimezone():
    def __init__(self, timezoneSTR):
        """
        Set default timezone for calculations
        timezoneSTR: str
            "US/Eastern"
            List can be obtained from pytz.all_timezones
        """
        self.zone = timezone(timezoneSTR)

    def getTimeStamp(self, format="%H:%M:%S %d-%b-%Y"):
        """
        Returns timestamp string
        format: str
            %H:%M:%S %d-%b-%Y (default)
        """
        return datetime.now(self.zone).strftime(format)

    def getDate(self, format="%m/%d/%Y"):
        """
        Returns date string
        format: str
            %m/%d/%Y (default)
        """
        return datetime.now(self.zone).strftime(format)

    def getDay(self, format="%A"):
        """
        Returns day string
        format: str
            %A (default)
        """
        return datetime.now(self.zone).strftime(format)

    def getEpoch(self, date=None, format="%m/%d/%Y"):
        """
        Returns timestamp int
        format: str
            %H:%M:%S %d-%b-%Y (default)
        """
        timestamp = datetime.now(self.zone) if date is None else datetime.strptime(date, format)
        return int(time.mktime(timestamp.timetuple()))

    def getRelativeDate(self, seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0, format="%Y-%m-%d"):
        """
        Returns date string
        seconds/minutes/hours/days/weeks/months/years: int
        format: str
            %Y-%m-%d (default)
        """
        return (datetime.utcnow() - relativedelta(seconds=seconds, minutes=minutes, hours=hours, days=days, weeks=weeks, months=months, years=years)).strftime(format)

    def getRelativeDateDT(self, seconds=0, minutes=0, hours=0, days=0, weeks=0, months=0, years=0):
        """
        Returns datetime object
        seconds/minutes/hours/days/weeks/months/years: int
        """
        return (datetime.utcnow() - relativedelta(seconds=seconds, minutes=minutes, hours=hours, days=days, weeks=weeks, months=months, years=years))

    def getTimeDiff(self, timestamp):
        """
        Returns time difference between provided timestamp and UTC time now
        """
        temp = datetime.fromtimestamp(int(timestamp))
        diff = datetime.combine(datetime.utcnow().date(), datetime.utcnow().time()) - datetime.combine(temp.date(), temp.time())
        hours = diff.days * 24 + diff.seconds / 3600
        return hours

    def compareDates(self, date1, date2, format1, format2):
        """
        Returns the difference between two dates in seconds, assuming date2 is greater than date1
        """
        date1 = datetime.strptime(date1, format1)
        date2 = datetime.strptime(date2, format2)

        difference = date2 - date1
        seconds = (difference.days * 86400) + difference.seconds
        return seconds
