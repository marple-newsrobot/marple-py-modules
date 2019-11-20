# encoding: utf-8

from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
import re
import requests_cache
import functools
import json
from decimal import Decimal
import numpy as np
import pandas as pd
from six import string_types, text_type

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

    elif periodicity == "weekly":
        return "week {}".format(timepoint.week)

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

    raise Exception(u"Unknown periodicity: '{}'".format(periodicity))

REGEX = {
    "yearly": "^\d\d\d\d$",
    "quarterly": "^\d\d\d\d-?[KQ][1-4]$",
    "monthly": "^\d\d\d\d-\d\d$",
}
def guess_periodicity(time_str):
    """ Guess periodicity from a date string
        "2015" => "yearly"
        "2015Q1" => "quarterly"
        "2015-K1" => "quarterly"
        "2015-01" => "monthly"
    """
    time_str = text_type(time_str)
    if re.match(REGEX["yearly"], time_str):
        return "yearly"
    elif re.match(REGEX["quarterly"], time_str):
            return "quarterly"
    elif re.match(REGEX["monthly"], time_str):
            return "monthly"
    else:
        raise ValueError(u"Unknown time string: '{}'".format(time_str))


def to_timepoint(time_str):
    """ Get the starting timepoint of period
        "2015" => "2015-01-01"
        "2015-03" => "2015-03-01"
    """
    time_str = text_type(time_str)
    if re.match("^\d\d\d\d$", time_str):
        # "2015"
        return time_str + "-01-01"
    elif re.match(REGEX["monthly"], time_str):
        # "2015-01"
        return time_str + "-01"
    elif re.match("^\d\d\d\d-\d\d-\d\d$", time_str):
        # 2015-01-01
        return time_str
    elif re.match(REGEX["quarterly"], time_str):
        year = time_str[:4]
        quarter_i = parse_int(time_str[4:].replace("K","").replace("Q","").replace("-",""))
        month = quarter_i * 3 - 2
        return "{}-{:02d}-01".format(year, month)
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
    elif periodicity in ["yearly", "school_year"]:
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
        files = next(os.walk(subdir))[2]
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
    if isinstance(x, string_types):
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

def parse_int(s):
    return int(float(s))


def parse_decimal(val):
    if val is None or pd.isna(val):
        return None
    elif isinstance(val, float):
        return Decimal(val)
    else:
        raise NotImplementedError(u"Unable to parse Decimal from {}".format(val))

def isNaN(num):
    """ Check if a value is nan
        http://stackoverflow.com/questions/944700/how-to-check-for-nan-in-python
    """
    return num != num

def multiple_and(*conditions):
    """Pass multiple conditions to

    >>> c_1 = data.col1 == True
    >>> c_2 = data.col2 < 64
    >>> c_3 = data.col3 != 4
    >>> data_filtered = data[multiple_and(c1,c2,c3)]
    https://stackoverflow.com/questions/13611065/efficient-way-to-apply-multiple-filters-to-pandas-dataframe-or-series
    """
    return functools.reduce(np.logical_and, conditions)

def cache_initiated():
    """Hackish function to test if there is an existing requests_cache"""
    try:
        requests_cache.get_cache()
        return True
    except AttributeError:
        return False


def get_decimal_encoder(prec=None, BaseEncoder=json.JSONEncoder):
    """Returns a JSONEncoder that forces Decimals to a given precision
    on string dump.

    Hackish worksround as these suggested solutions did not work in python3:
    https://stackoverflow.com/questions/1447287/format-floats-with-standard-json-module

    Example usage:

        data = {
            "value": Decimal(1.543),
        }
        print(json.dumps(data, cls=get_decimal_encoder(2)))
        # '{"value": 1.54}'

    :param prec (int): number of decimals
    """
    class DecimalEncoder(BaseEncoder):
        def default(self, o):
            if isinstance(o, Decimal):
                p = self.PRECISION
                if p is not None:
                    # Make the decimal a string with N decimal and then float it
                    o_str ='%.{0}f'.format(p) % o
                    return float(o_str)
                else:
                    return float(o)
            return super(DecimalEncoder, self).default(o)

    DecimalEncoder.PRECISION = prec

    return DecimalEncoder
