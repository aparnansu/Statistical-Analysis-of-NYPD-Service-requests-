import csv
import logging
from datetime import datetime

from mrjob.job import MRJob
from mrjob.util import log_to_stream
import calendar

# Complaint status
OPEN = 'Open'
CLOSED = 'Closed'
ASSIGNED = 'Assigned'
# Complaint values
STREET_CONDITION = 'Street Condition'
ILLEGAL_PARKING = 'Illegal Parking'
NOISE_COMPLAINT = 'Noise Complaint'
NOISE = 'noise'
# CSV Fields
COMPLAINT_TYPE = 'Complaint Type'
STATUS = 'Status'
CREATED_DATE = 'Created Date'
# Key identifiers
QUARTER = "Quarter"
BOROUGH = "Borough"
TOTAL_NO_OF = "Total number of"
MONTH = "Month"
# Variable array
STATUSES = [OPEN, ASSIGNED, CLOSED]
COMPLAINT_TYPES = [STREET_CONDITION, ILLEGAL_PARKING, NOISE_COMPLAINT]
YEAR_TO_FILTER = 2017

log = logging.getLogger(__name__)


class MRWordFrequencyCount(MRJob):

    def set_up_logging(cls, quiet=False, verbose=False, stream=None):
        log_to_stream(name='mrjob', debug=verbose, stream=stream)
        log_to_stream(name='__main__', debug=verbose, stream=stream)

    def mapper(self, _, line):
        cols = 'Unique Key,Created Date,Closed Date,Agency,Agency Name,Complaint Type,Descriptor,Location Type,' \
               'Incident Zip,Incident Address,Street Name,Cross Street 1,Cross Street 2,Intersection Street 1,' \
               'Intersection Street 2,Address Type,City,Landmark,Facility Type,Status,Due Date,Resolution ' \
               'Description,Resolution Action Updated Date,Community Board,BBL,Borough'.split(',')
        status = None
        complaint_type = None
        borough = None
        quarter = 0
        year = None
        month = None

        try:
            for row in csv.reader([line]):
                dict_row = dict(zip(cols, row))
                complaint_type = dict_row[COMPLAINT_TYPE]
                status = dict_row[STATUS]
                date_obj = datetime.strptime(dict_row[CREATED_DATE], '%m/%d/%Y %H:%M:%S %p')
                year = date_obj.year
                quarter = self.get_quarter_of_date(date_obj)
                borough = dict_row[BOROUGH]
                month = date_obj.month

        except Exception:
            log.info("Skipping the line " + line)
        else:
            # Total complaints
            for complaint_var in COMPLAINT_TYPES:
                total_complaints = 0
                for status_var in STATUSES:
                    count = 0
                    # If status matches and complaint matches set the count to 1
                    if status_var == status:
                        if complaint_type.lower() == complaint_var.lower():
                            count = 1
                        elif complaint_var == NOISE_COMPLAINT and complaint_type.lower().startswith(NOISE):
                            count = 1
                    total_complaints += count
                    if count > 0:
                        yield "{} {} {} complaints".format(TOTAL_NO_OF, status_var, complaint_var), count

                # Monthly calculations
                for month_var in range(1, 13):
                    m_count = 0
                    if month_var == month and count == 1 and year == YEAR_TO_FILTER:
                        m_count = total_complaints
                    if m_count > 0:
                        yield "{} wise [{}] {} complaints".format(MONTH, calendar.month_name[month_var], complaint_var), m_count

                # Quarter wise calculations
                for quarter_var in range(1, 5):
                    q_count = 0
                    if quarter_var == quarter and count == 1 and year == YEAR_TO_FILTER:
                        q_count = 1
                    if q_count > 0:
                        yield "{} {} {} {} complaints".format(QUARTER, str(quarter_var), status_var, complaint_var), q_count

            # Borough with illegal parking
            for quarter_var in range(1, 5):
                q_count = 0
                if quarter_var == quarter and complaint_type.lower() == ILLEGAL_PARKING.lower() and year == YEAR_TO_FILTER:
                    q_count = 1
                if q_count > 0:
                    yield "{} with {} for the quarter [{}]".format(BOROUGH, ILLEGAL_PARKING, str(quarter_var)), (borough, q_count)

    def reducer(self, key, values):
        if key.startswith(BOROUGH):
            result_dict = dict()
            for item in list(values):
                borough = item[0]
                count = item[1]
                result_dict[borough] = result_dict.get(borough, 0) + count
            yield key, [(k, v) for (k, v) in result_dict.items() if v == max(result_dict.values())]
        elif key.startswith(QUARTER):
            yield key, int(sum(values) // 3)
        else:
            yield key, sum(values)

    def get_quarter_of_date(self, date_obj):
        """
        Assign created date to a variable
        Convert string to date
        Extract month from date
        (month-1)/3 +1
        """
        month = date_obj.month
        quarter = ((month - 1) // 3) + 1
        return quarter

if __name__ == '__main__':
    MRWordFrequencyCount.run()
