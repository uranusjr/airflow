# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Tests for TimeTable.iter_between() and TimeTable.iter_next_n()."""

from datetime import datetime, timedelta

import pytest

from airflow.settings import TIMEZONE
from airflow.timetables.interval import DeltaDataIntervalTimeTable
from airflow.utils.timezone import is_localized


@pytest.fixture()
def time_table_1s():
    return DeltaDataIntervalTimeTable(timedelta(seconds=1))


def test_end_date_before_start_date(time_table_1s):
    start = datetime(2016, 2, 1, tzinfo=TIMEZONE)
    end = datetime(2016, 1, 1, tzinfo=TIMEZONE)
    message = r"start \([- :+\d]{25}\) > end \([- :+\d]{25}\)"
    with pytest.raises(ValueError, match=message):
        list(time_table_1s.iter_between(start, end))


@pytest.mark.parametrize("num", list(range(1, 10)))
def test_positive_num_given(time_table_1s, num):
    start = datetime(2016, 1, 1, tzinfo=TIMEZONE)
    result = list(time_table_1s.iter_next_n(start, num))
    assert len(result) == num
    assert all(is_localized(d) for d in result)
