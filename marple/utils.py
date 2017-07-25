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

def parse_lingual_object(str_or_dict, lang=None, prefix=None, fallback_chain=["en","sv"]):
    """ Json objects my contain strings in multiple languages.
        This

        Eg.
        parse_lingual_object({"en": "dog", "sv": "hund"}, "en") => "dog"
        parse_lingual_object({"en": "dog", "sv": "hund"}, "sv") => "hund"
        parse_lingual_object({"en": "dog", "sv": "hund"}, None) => "dog"
        parse_lingual_object({
            "label__en": "dog",
            "label__sv": "hund"}, "sv", "label") => "hund"
        parse_lingual_object("dog") => "dog"

        :param str_or_dict: string or dict to parse
        :param prefix: ie. "label" or "description" if key name is "label__en"
        :param fallback_chain: In case no language is defined, pick
            one of the fallback langagues.
        :returns: a translated string
    """
    #
    x = str_or_dict

    # 1. If string, return as such
    if isinstance(x, str) or isinstance(x, unicode):
        return x

    #if lang is None and prefix is not None:
    #    return x[prefix]

    # 2. Parse dict
    elif isinstance(x, dict):
        # 2.1 Parse using defined languagre
        if lang is not None:
            try:
                if prefix is not None:
                    key = "{}__{}".format(prefix, lang)
                else:
                    key = lang
                return x[key]
            except KeyError:
                raise KeyError("Unable to translate '{}' with '{}'"\
                    .format(x, key))


        else:
            # 2.2 Check if there is a default ie "label" (without any
            # language key)
            if prefix is not None and prefix in x:
                return x[prefix]

            # 2.3 Try fallback languages
            for fallback_lang in fallback_chain:
                if prefix is not None:
                    key = "{}__{}".format(prefix, fallback_lang)
                else:
                    key = fallback_lang
                try:
                    return x[key]
                except KeyError:
                    pass

    raise ValueError("Unable to translate '{}' with '{}'"\
            .format(x,lang))


def isNaN(num):
    """ Check if a value is nan
        http://stackoverflow.com/questions/944700/how-to-check-for-nan-in-python
    """
    return num != num
