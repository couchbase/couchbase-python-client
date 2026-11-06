#  Copyright 2016-2026. Couchbase, Inc.
#  All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License")
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from __future__ import annotations

import json
import logging
from collections.abc import Callable
from typing import Any, cast

import pytest

from couchbase.common import _redaction
from couchbase.common._observability.logging_meter import LoggingMeter, LoggingMeterReporter
from couchbase.common._observability.threshold_logging import ThresholdLoggingReporter, ThresholdLogRecord
from couchbase.common._operations import ServiceType
from couchbase.common._redaction import (escape,
                                         metadata,
                                         system_data,
                                         user_data)


@pytest.fixture(name='redaction')
def redaction_fixture(monkeypatch: pytest.MonkeyPatch) -> Callable[[bool], None]:
    # The flag is process-wide and never cleared, so a test that turns it on through `enable()`
    # would leave every later test in the worker redacting.  Setting it through monkeypatch puts
    # it back however the test ends.
    def _set(enabled: bool) -> None:
        monkeypatch.setattr(_redaction, '_enabled', enabled)

    _set(False)
    return _set


class LogRedactionUnitTests:
    # No cb_env: the tags are pure Python and are checked against the format the core writes.

    @pytest.mark.parametrize('value, expected', [
        ('plain', 'plain'),
        ('', ''),
        ('a\nb', 'a\\nb'),
        ('a\rb', 'a\\rb'),
        ('a\r\nb', 'a\\r\\nb'),
        ('<ud>x</ud>', '\\u003cud>x\\u003c/ud>'),
        ('<md></md><sd></sd>', '\\u003cmd>\\u003c/md>\\u003csd>\\u003c/sd>'),
        ('a < b', 'a < b'),
        ('<div>', '<div>'),
        ('<u', '<u'),
        ('<<ud>', '<\\u003cud>'),
    ])
    def test_escape(self, value: str, expected: str) -> None:
        assert escape(value) == expected

    @pytest.mark.parametrize('wrap, tag', [(user_data, 'ud'), (metadata, 'md'), (system_data, 'sd')])
    def test_tags_only_while_enabled(self,
                                     redaction: Callable[[bool], None],
                                     wrap: Callable[[Any], object],
                                     tag: str) -> None:
        tagged = wrap('key\n<ud>')
        assert str(tagged) == 'key\n<ud>'
        redaction(True)
        assert str(tagged) == f'<{tag}>key\\n\\u003cud></{tag}>'

    def test_tags_when_the_record_is_formatted(self, redaction: Callable[[bool], None]) -> None:
        # Built before the flag is set and formatted after, which is how a record queued by a
        # handler behaves: what decides is the flag at format time.
        record = logging.LogRecord('couchbase', logging.INFO, __file__, 0, '%s and %s', (user_data(1), 'x'), None)
        redaction(True)
        assert record.getMessage() == '<ud>1</ud> and x'

    def test_enable(self, redaction: Callable[[bool], None]) -> None:
        assert not _redaction.is_enabled()
        _redaction.enable()
        assert _redaction.is_enabled()

    @pytest.mark.parametrize('enabled', [False, True])
    def test_threshold_report_is_system_data(self,
                                             redaction: Callable[[bool], None],
                                             caplog: pytest.LogCaptureFixture,
                                             enabled: bool) -> None:
        redaction(enabled)
        reporter = ThresholdLoggingReporter(interval=100)
        record: ThresholdLogRecord = {'operation_name': 'get', 'total_duration_us': 1000.0}
        reporter.add_log_record(ServiceType.KeyValue, record, 1000)

        with caplog.at_level(logging.INFO, logger='couchbase.threshold'):
            reporter._report()

        message = caplog.records[-1].getMessage()
        if enabled:
            assert message.startswith('<sd>') and message.endswith('</sd>')
            message = message[len('<sd>'):-len('</sd>')]
        assert json.loads(message)[ServiceType.KeyValue.value]['top_requests'] == [record]

    @pytest.mark.parametrize('enabled', [False, True])
    def test_metrics_report_is_metadata(self,
                                        redaction: Callable[[bool], None],
                                        caplog: pytest.LogCaptureFixture,
                                        enabled: bool) -> None:
        redaction(enabled)
        report = {'meta': {'emit_interval_s': 600}, 'operations': {'kv': {}}}

        class _Meter:
            def create_report(self) -> dict[str, Any]:
                return report

        reporter = LoggingMeterReporter(logging_meter=cast(LoggingMeter, _Meter()), interval=100)
        with caplog.at_level(logging.INFO, logger='couchbase.metrics'):
            reporter._report()

        message = caplog.records[-1].getMessage()
        if enabled:
            assert message.startswith('<md>') and message.endswith('</md>')
            message = message[len('<md>'):-len('</md>')]
        assert json.loads(message) == report
