from datetime import datetime  
import pytz

"""Some datetime utilities."""

def getCurrentWeekNumber():
    """Get the current week number."""
    today = datetime.today()   
    return today.isocalendar()[1]

def getCurrentYear():
    """Get the current year."""
    today = datetime.today()   
    return today.isocalendar()[0]

def getDatetimeSpanForWeekNumber(year, week_num):
    """Get the open and close datetimes for a week.
    Args:
        year: The year.
        week_num: The week number.
    Returns:
        A tuple of (open_datetime, close_datetime).
    """

    local = pytz.timezone("America/Los_Angeles")

    # Get openijng time for Monday morning
    d = f'{year}-W{week_num}'
    monday_dt = datetime.strptime(d + '-1', '%G-W%V-%u')
    month = monday_dt.month
    day = monday_dt.day
    naive = datetime.strptime(f"{year}-{month:02d}-{day:02d} 06:30:00", "%Y-%m-%d %H:%M:%S")
    local_dt = local.localize(naive, is_dst=None)
    monday_utc_time = local_dt.astimezone(pytz.utc)

    # Get closing time for Friday afternoon
    friday_dt = datetime.strptime(d + '-5', '%G-W%V-%u')
    month = friday_dt.month
    day = friday_dt.day
    naive = datetime.strptime(f"{year}-{month:02d}-{day:02d} 13:00:00", "%Y-%m-%d %H:%M:%S")
    local_dt = local.localize(naive, is_dst=None)
    friday_utc_time = local_dt.astimezone(pytz.utc)

    return (monday_utc_time.strftime("%Y-%m-%dT%H:%M:%SZ"), 
            friday_utc_time.strftime("%Y-%m-%dT%H:%M:%SZ"))

if __name__ == "__main__":
    print(getDatetimeSpanForWeekNumber(2023, 1))
    #print(getDatetimeSpanForWeekNumber(2023, 22))
    #print(f'Cureent week is: {getCurrentWeekNumber()}')
    #this_week_span = getDatetimeSpanForWeekNumber(getCurrentYear(), getCurrentWeekNumber())
    #print(f"Current week span: {this_week_span[0]} to {this_week_span[1]}")
