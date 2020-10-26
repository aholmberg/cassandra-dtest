from abc import ABC, abstractmethod
from collections import defaultdict
import time
import pytest
import parse
import re
import logging

from cassandra.query import SimpleStatement

from cassandra import ConsistencyLevel as CL
import ccmlib.common
from ccmlib.node import ToolError

from distutils.version import LooseVersion

from dtest import Tester
from tools.jmxutils import (JolokiaAgent, enable_jmx_ssl, make_mbean)
from tools.misc import generate_ssl_stores

since = pytest.mark.since
logger = logging.getLogger(__name__)

jmx = None


def diff_num(v1, v2):
    return v2 - v1


def diff_str(v1, v2):
    return f"'{v1}' --> '{v2}'"


diff_function = {
    int: diff_num,
    float: diff_num,
    list: diff_str,
    str: diff_str,
}


def diff_value(v1, v2):
    if v1 is None or v2 is None:  # Before it's set, Mean "null" is returned as None
        return diff_str(v1, v2)
    return diff_function[type(v1)](v1, v2)


class AbstractPropertyValues(ABC):
    def __init__(self, scope, name):
        self.scope = scope
        self.name = name
        self.mbean = make_mbean('metrics', type='ClientRequest', scope=scope, name=name)
        self.values = {}
        self.init()

    @abstractmethod
    def init(self):
        pass

    def load(self, attr):
        self.values[attr] = jmx.read_attribute(self.mbean, attr)

    def diff(self, other):
        d = {}
        for k, v in self.values.items():
            if 'RecentValues' in k:
                continue
            v2 = other.values[k]
            if v != v2:
                key = f"{self.name}.{k}"
                d[key] = diff_value(v, v2)
        return d


class CounterValue(AbstractPropertyValues):
    def init(self):
        self.load("Count")


class MeterValue(AbstractPropertyValues):
    def init(self):
        for a in ["Count",
                  "MeanRate",
                  "OneMinuteRate",
                  "FiveMinuteRate",
                  "FifteenMinuteRate",
                  "RateUnit"]:
            self.load(a)


stat_words = [
    'Min',
    'Max',
    'Mean',
    'StdDev',
    '50thPercentile',
    '75thPercentile',
    '95thPercentile',
    '98thPercentile',
    '99thPercentile',
    '999thPercentile',
    'RecentValues']


class HistogramValue(AbstractPropertyValues):
    def init(self):
        for a in stat_words:
            self.load(a)
        self.load('Count')


class LatencyTimerValue(CounterValue):

    def init(self):
        super(LatencyTimerValue, self).init()
        for a in stat_words:
            self.load(a)
        self.load('DurationUnit')


class LatencyMetrics(object):
    def __init__(self, scope):
        self.scope = scope
        self.values = {'TotalLatency': CounterValue(scope, 'TotalLatency'),
                       'Latency': LatencyTimerValue(scope, 'Latency')}

    def diff(self, other):
        d = {}
        for k, v in self.values.items():
            d.update(other.values[k].diff(v))
        return d


class RequestMetrics(LatencyMetrics):
    def __init__(self, scope):
        super(RequestMetrics, self).__init__(scope)
        for mb in ['Failures', 'Timeouts', 'Unavailables']:
            self.values[mb] = MeterValue(scope, mb)


class WriteMetrics(RequestMetrics):
    def __init__(self, scope):
        super(WriteMetrics, self).__init__(scope)
        self.values['MutationSizeHistogram'] = HistogramValue(scope, 'MutationSizeHistogram')


class TestClientRequestMetrics(Tester):

    def test_write_metrics(self):
        cluster = self.cluster
        cluster.populate(1, debug=True)
        node1 = cluster.nodelist()[0]
        cluster.start()

        global jmx
        jmx = JolokiaAgent(node1)
        jmx.start()

        def scope_for_cl(cl):
            return 'Write-' + CL.value_to_name[cl]

        def is_zero(v):
            assert v == 0

        def is_positive(v):
            assert v > 0

        def is_none(v):
            assert v is None

        def is_microseconds(v):
            assert v == 'microseconds'

        def is_zero_list(l):
            assert not any(l)

        def is_nonzero_list(l):
            assert any(l)

        def is_histo_list(l):
            # since these values change on sampling, we can only generally verify it takes the proper form
            # there is a dedicated test to make sure the histogram behaves as expected.
            assert len(l) == 165
            assert all(isinstance(i, int) for i in l)

        def validate_zero_latency(latency_values):
            validators = defaultdict(lambda : is_zero)
            validators['RecentValues'] = is_zero_list
            validators['Mean'] = is_none
            validators['DurationUnit'] = is_microseconds
            for k,v in latency_values.items():
                validators[k](v)

        def validate_sane_latency(latency_values):
            validators = defaultdict(lambda : is_positive)
            validators['RecentValues'] = is_histo_list
            validators['DurationUnit'] = is_microseconds
            if latency_values['Count'] == 1:
                validators['StdDev'] = is_zero
            for k,v in latency_values.items():
                validators[k](v)

            assert latency_values['Min'] <= latency_values['Max']
            assert latency_values['Min'] <= latency_values['Mean']
            assert latency_values['Mean'] <= latency_values['Max']

            last_pct = latency_values['50thPercentile']
            for s in ['75th', '95th', '98th', '99th', '999th']:
                this_pct = latency_values[f"{s}Percentile"]
                assert this_pct >= last_pct
                last_pct = this_pct

        def validate_write_metrics(write_metrics):
            assert write_metrics.values['TotalLatency'].values['Count'] >= 0

            latency_values = write_metrics.values['Latency'].values
            sample_count = latency_values['Count']
            assert sample_count >= 0
            if sample_count:
                validate_sane_latency(latency_values)
            else:
                validate_zero_latency(latency_values)

            # no failures expected
            for mbean in ['Failures', 'Timeouts', 'Unavailables']:
                for k,v in write_metrics.values[mbean].values.items():
                    if k == 'RateUnit':
                        assert v == 'events/second'
                    else:  # Count, mean/1/5/15 Rates
                        assert v == 0

        session = self.patient_cql_connection(node1)

        session.execute("CREATE KEYSPACE ks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}")
        session.execute("CREATE TABLE if not exists ks.t (k int PRIMARY KEY, v int)")

        # don't race with async default role creation, which creates a write
        self.assert_log_had_msg(node1, "Created default superuser role 'cassandra'")

        query_cl = CL.ONE
        # Validate other CLs metrics are not changing. We're not messing with Quorum because of that role write above.
        # If the test takes long enough, it's possible to see some of the time-based metrics shift.
        other_cls = [c for c in CL.value_to_name if c not in (query_cl, CL.QUORUM)]
        global_baseline = WriteMetrics('Write')
        cl_baseline = WriteMetrics(scope_for_cl(query_cl))
        other_baselines = [WriteMetrics(scope_for_cl(c)) for c in other_cls]

        validate_write_metrics(global_baseline)
        validate_write_metrics(cl_baseline)
        for b in other_baselines:
            validate_write_metrics(b)

        query_count = 5
        ss = SimpleStatement("INSERT INTO ks.t (k,v) VALUES (0,0)", consistency_level=query_cl)
        for _ in range(query_count):
            session.execute(ss)

        global_updated = WriteMetrics('Write')
        cl_updated = WriteMetrics(scope_for_cl(query_cl))
        validate_write_metrics(global_baseline)
        validate_write_metrics(cl_baseline)

        global_diff = global_updated.diff(global_baseline)
        cl_diff = cl_updated.diff(cl_baseline)
        for diff in [global_diff, cl_diff]:
            assert diff['TotalLatency.Count'] > 0
            assert diff['Latency.Count'] == query_count
            assert diff['MutationSizeHistogram.Count'] == query_count

        other_updated = [WriteMetrics(scope_for_cl(c)) for c in other_cls]
        for updated, baseline in zip(other_updated, other_baselines):
            assert updated.diff(baseline) == {}, updated.scope
