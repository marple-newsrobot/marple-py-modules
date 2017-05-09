# encoding: utf-8

from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import re


def get_timepoint_label(datestring, periodicity):
    """ Convert a datestring to a timepoint label.
        :param datestring: an iso coded datestring. E.g. "2016-01-01"
        :param periodicity: monthly|quarterly|yearly|rolling_quarter|rolling_year
        :returns: a label for the timepoint
        :rtype: str 
    """
    timepoint = datetime.strptime(datestring, '%Y-%m-%d')

    if periodicity == "monthly":
        # Jan 2016
        return timepoint.strftime("%b %Y")

    elif periodicity == "quarterly":
        # Q1 2016
        quarters = [0, 1, 1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4]
        return "Q{} {}".format(
            quarters[timepoint.month],
            timepoint.year)

    elif periodicity == "yearly":
        # 2016
        return str(timepoint.year)

    elif periodicity == "rolling_quarter":
        # 2016-03-01 => "Jan 2016-Mar 2016"
        timepoint_start = timepoint - relativedelta(months=2)
        return "{}-{}".format(
            timepoint_start.strftime("%b %Y"),
            timepoint.strftime("%b %Y"))

    elif periodicity == "rolling_year":
        # 2017-01-01 => "Feb 2016-Jan 2017"
        timepoint_start = timepoint - relativedelta(months=11)
        return "{}-{}".format(
            timepoint_start.strftime("%b %Y"),
            timepoint.strftime("%b %Y"),
        )


def guess_periodicity(time_str):
    """ Guess periodicity from a date string
        "2015" => "yearly"
        "2015-01" => "monthly"
    """
    time_str = unicode(time_str)
    if re.match("^\d\d\d\d$", time_str):
        return "yearly"
    elif re.match("^\d\d\d\d-\d\d$", time_str):
        return "monthly"
    else:
        raise ValueError(u"Unknown time string: '{}'".format(time_str))


def to_timepoint(time_str):
    """ Get the starting timepoint of period
        "2015" => "2015-01-01"
        "2015-03" => "2015-03-01"
    """
    time_str = unicode(time_str)
    if re.match("^\d\d\d\d$", time_str):
        # "2015"
        return time_str + "-01-01"
    elif re.match("^\d\d\d\d-\d\d$", time_str):
        # "2015-01"
        return time_str + "-01"
    elif re.match("^\d\d\d\d-\d\d-\d\d$", time_str):
        # 2015-01-01
        return time_str
    else:
        raise ValueError(u"Unknown time string: '{}'".format(time_str))



def subtract_periods(timepoint, n_periods, periodicity=None):
    """ Subtract n number of periods from a timepoint
    Example usage:
    
        subtract_periods(2015, 2, "yearly") => "2013-01-01"
        subtract_periods("2015-03", 2, "monthly") => "2015-01-01"
        subtract_periods("2015-07-01", 6, "monthly") => "2015-01-01"

    :param timepoint (str|int): a timepoint
    :param n_periods (str): number of periods 
    :param periodicity (str): "monthly"|"yearly"|"rolling_quarter"|"rolling_year"
    :returns: computed timepoint as string
    """
    if periodicity is None:
        periodicity = guess_periodicity(timepoint)

    timepoint = to_timepoint(timepoint)
    dt = datetime.strptime(timepoint, "%Y-%m-%d")

    if periodicity in ["monthly", "rolling_quarter", "rolling_year"]:
        dt -= relativedelta(months=n_periods)
    elif periodicity == "yearly":
        dt -= relativedelta(years=n_periods)
    else:
        msg = u"Unable to subtract periods for periodicity '{}'".format(periodicity)
        NotImplementedError(msg)

    return datetime.strftime(dt, "%Y-%m-%d")


def list_files(dir, extension=None, file_name=None):                                                                                                  
    """ Get a list of all files in directory and subdirectories
        :param dir: path to direcotyr to parse
        :param extension: Only include files with given extension.
            Both ".txt" and "txt" allowed.
    """
    r = []
    subdirs = [x[0] for x in os.walk(dir)]
    for subdir in subdirs:
        files = os.walk(subdir).next()[2]
        if (len(files) > 0):
            for file in files:
                _file_name, _file_extension = os.path.splitext(file)

                if extension != None:
                    _file_extension = _file_extension.replace(".","")
                    extension = extension.replace(".","")

                    if extension != _file_extension:
                        continue

                if file_name != None:
                    if file_name != file:
                        continue

                r.append(subdir + "/" + file)

    return r 



def isNaN(num):
    """ Check if a value is nan
        http://stackoverflow.com/questions/944700/how-to-check-for-nan-in-python
    """
    return num != num
