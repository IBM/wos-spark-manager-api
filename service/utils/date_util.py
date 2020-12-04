# ----------------------------------------------------------------------------------------------------
# (C) Copyright IBM Corp. 2020.  All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
# an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
# specific language governing permissions and limitations under the License.
# ----------------------------------------------------------------------------------------------------

import datetime
import time


class DateUtil:

    date_format = "%Y-%m-%dT%H:%M:%S.%fZ"
    date_format_alt = "%Y-%m-%dT%H:%M:%SZ"

    @classmethod
    def get_datetime_str_as_time(cls, str_time: str = None):
        if str_time is None:
            return datetime.datetime.strptime(DateUtil.get_current_datetime(), cls.date_format)

        try:
            return datetime.datetime.strptime(str_time, cls.date_format)
        except ValueError:
            date_format = cls.date_format_alt
            if "T" not in str_time:
                date_format = "%Y-%m-%d %H:%M:%S"
                if "." in str_time:
                    date_format = "%Y-%m-%d %H:%M:%S.%f"
            return datetime.datetime.strptime(str_time, date_format)

    @classmethod
    def get_current_datetime(cls):
        now = datetime.datetime.utcnow()
        return now.strftime(cls.date_format)

    @classmethod
    def get_datetime_as_str(cls, date_time, format = None):
        if format is None:
            return date_time.strftime(cls.date_format)
        else:
            return date_time.strftime(format)

    @classmethod
    def get_current_datetime_alt(cls):
        now = datetime.datetime.utcnow()
        return now.strftime(cls.date_format_alt)

    @classmethod
    def current_milli_time(cls):
        return round(time.time() * 1000)

    @classmethod
    def get_current_datetime_with_offset(cls, offset: float):
        """
        returns current time offset by given milliseconds
        """
        now = datetime.datetime.utcnow()
        now = now + datetime.timedelta(milliseconds=offset)
        return now.strftime(cls.date_format)

    @classmethod
    def get_datetime_with_time_delta(cls, time: str, unit: str, count, previous: bool = False):
        """
        returns time delta calculated as per unit and count
        """
        time = DateUtil.get_datetime_str_as_time(time)

        difference = datetime.timedelta()
        if unit == "day":
            difference = datetime.timedelta(days=count)
        elif unit == "hour":
            difference = datetime.timedelta(hours=count)
        elif unit == "minute":
            difference = datetime.timedelta(minutes=count)
        elif unit == "second":
            difference = datetime.timedelta(seconds=count)
        elif unit == "microsecond":
            difference = datetime.timedelta(microseconds=count)

        time = (time + difference) if not previous else (time - difference)
        return time.strftime(cls.date_format)

    @classmethod
    def get_time_diff_in_seconds(cls, from_time: str, to_time: str = None):
        if to_time is None:
            current_timestamp = DateUtil.get_datetime_str_as_time()
        else:
            current_timestamp = DateUtil.get_datetime_str_as_time(to_time)
        return (current_timestamp - DateUtil.get_datetime_str_as_time(from_time)).total_seconds()

    @classmethod
    def get_time_diff_in_minutes(cls, from_time: str, to_time: str = None):
        time_diff_in_seconds = cls.get_time_diff_in_seconds(from_time, to_time)
        return time_diff_in_seconds / 60

    @classmethod
    def get_time_diff_in_hours(cls, from_time: str, to_time: str = None):
        time_diff_in_seconds = cls.get_time_diff_in_seconds(from_time, to_time)
        return time_diff_in_seconds / (60 * 60)

    @classmethod
    def get_time_diff_in_days(cls, from_time: str, to_time: str = None):
        time_diff_in_seconds = cls.get_time_diff_in_seconds(from_time, to_time)
        return time_diff_in_seconds / (24 * 60 * 60)

    @classmethod
    def is_source_time_greater(cls, source: str, target: str):
        source_datetime = DateUtil.get_datetime_str_as_time(source)
        target_datetime = DateUtil.get_datetime_str_as_time(target)

        if source_datetime > target_datetime:
            return True

        return False

    @classmethod
    def get_datetime_from_milliseconds(cls, epoch_milliseconds):
        seconds = epoch_milliseconds / 1000.0
        return datetime.datetime.fromtimestamp(seconds).strftime(cls.date_format)

    @classmethod
    def get_utc_datetime_from_milliseconds(cls, epoch_milliseconds):
        seconds = epoch_milliseconds / 1000.0
        return datetime.datetime.utcfromtimestamp(seconds).strftime(cls.date_format)

    @classmethod
    def get_max_time_as_str(cls):
        now = datetime.datetime.max
        return now.strftime(cls.date_format)

    @classmethod
    def get_current_datetime_lineage_format(cls):
        now = datetime.datetime.utcnow()
        return now.strftime("%Y-%m-%dT%H:%M:%SZ")
