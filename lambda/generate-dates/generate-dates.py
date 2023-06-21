from datetime import datetime
from datetime import timedelta


def lambda_handler(event, context):
    """
    Generate a list of dates (string format)
    """

    BEGIN_DATE_STR = "20180101"
    END_DATE_STR = "20181231"

    # carry out conversion between string
    # to datetime object
    current_date = datetime.strptime(BEGIN_DATE_STR, "%Y%m%d")
    end_date = datetime.strptime(END_DATE_STR, "%Y%m%d")

    result = []

    while current_date <= end_date:
        current_date_str = current_date.strftime("%Y%m%d")

        result.append(current_date_str)

        # adding 1 day
        current_date += timedelta(days=1)

    return result
