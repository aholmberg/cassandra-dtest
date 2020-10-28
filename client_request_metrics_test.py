from abc import ABC, abstractmethod
from collections import defaultdict
import time
import pytest
import parse
import re
import logging
from datetime import datetime

from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.policies import NeverRetryPolicy
from cassandra.query import SimpleStatement

from cassandra import ConsistencyLevel as CL
from cassandra import (ReadFailure, ReadTimeout, Unavailable,
                       WriteFailure, WriteTimeout)
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

    def validate_zeros(self):
        assert self.values['RateUnit'] == 'events/second'
        for k,v in self.values.items():
            if k == 'RateUnit':
                continue
            assert v == 0


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

    def validate(self):
        latency_values = self.values['Latency'].values
        sample_count = self.values['Count']
        assert sample_count >= 0
        if sample_count:
            self.validate_sane_latency()
        else:
            self.validate_zero_latency()

    def validate_sane_latency(self):
        values = self.values
        validators = defaultdict(lambda: is_positive)
        validators['RecentValues'] = is_histo_list
        validators['DurationUnit'] = is_microseconds
        if latency_values['Count'] == 1:
            validators['StdDev'] = is_zero
        for k, v in values.items():
            validators[k](v)

        assert values['Min'] <= values['Max']
        assert values['Min'] <= values['Mean']
        assert values['Mean'] <= values['Max']

        last_pct = values['50thPercentile']
        for s in ['75th', '95th', '98th', '99th', '999th']:
            this_pct = values[f"{s}Percentile"]
            assert this_pct >= last_pct
            last_pct = this_pct

    def validate_zero_latency(self):
        validators = defaultdict(lambda: is_zero)
        validators['RecentValues'] = is_zero_list
        validators['Mean'] = is_none
        validators['DurationUnit'] = is_microseconds
        for k, v in self.values.items():
            validators[k](v)


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

    def validate(self):
        self.values['Latency'].validate()


class RequestMetrics(LatencyMetrics):
    error_words = ['Failures', 'Timeouts', 'Unavailables']

    def __init__(self, scope):
        super(RequestMetrics, self).__init__(scope)
        for mb in self.error_words:
            self.values[mb] = MeterValue(scope, mb)

    def validate(self):
        # no failures expected
        for mb in self.error_words:
            self.values[mb].validate_zeros()


class WriteMetrics(RequestMetrics):
    def __init__(self, scope):
        super(WriteMetrics, self).__init__(scope)
        self.values['MutationSizeHistogram'] = HistogramValue(scope, 'MutationSizeHistogram')

    def validate(self):
        super(WriteMetrics, self).validate()
        #todo: something about MutationSizeHistogram


def scope_for_cl(scope, cl):
    return scope + '-' + CL.value_to_name[cl]

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


KEYSPACE = 'ks'
FAIL_WRITE_KEYSPACE = 'fail_keyspace'
RF = 2
TABLE = 't'
TOMBSTONE_FAILURE_THRESHOLD = 20

class TestClientRequestMetrics(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            "Testing write failures",  # The error to simulate a write failure
            "ERROR WRITE_FAILURE",  # Logged in DEBUG mode for write failures
        )

    def setup_once(self):
        cluster = self.cluster
        cluster.set_configuration_options({'read_request_timeout_in_ms': 3000,
                                           'write_request_timeout_in_ms': 3000,
                                           'phi_convict_threshold': 12,
                                           'tombstone_failure_threshold': TOMBSTONE_FAILURE_THRESHOLD})
        cluster.populate(2, debug=True)
        node1 = cluster.nodelist()[0]
        cluster.start(jvm_args=["-Dcassandra.test.fail_writes_ks=" + FAIL_WRITE_KEYSPACE])

        global jmx
        jmx = JolokiaAgent(node1)
        jmx.start()

        s = self.session = self.patient_exclusive_cql_connection(node1)
        for k in [KEYSPACE, FAIL_WRITE_KEYSPACE]:
            s.execute(f"CREATE KEYSPACE {k} WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '{RF}'}}")
            s.execute(f"CREATE TABLE {k}.{TABLE} (k int, c int, v int, PRIMARY KEY (k, c))")

        node1.watch_log_for("Created default superuser role 'cassandra'")  # don't race with async default role creation, which creates a write
        node1.watch_log_for("Completed submission of build tasks for any materialized views defined at startup", filename='debug.log')  # view builds cause background reads

    def test_client_request_metrics(self):
        self.setup_once()
        self.write_happy_path()
        self.read_happy_path()
        self.write_failures()
        self.write_unavailables()
        self.write_timeouts()
        self.read_failures()
        self.read_unavailables()
        self.read_timeouts()

    def write_happy_path(self):
        query_count = 5
        global_diff, cl_diff = self.validate_happy_path('Write',
                                                        WriteMetrics,
                                                        SimpleStatement(f"INSERT INTO {KEYSPACE}.{TABLE} (k,c) VALUES (0,0)", consistency_level=CL.ONE),
                                                        query_count)

        assert global_diff['MutationSizeHistogram.Count'] == query_count

    def read_happy_path(self):
        query_count = 5
        self.validate_happy_path('Read',
                                 RequestMetrics,
                                 SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0 AND c=0", consistency_level=CL.LOCAL_ONE),
                                 query_count)

    def validate_happy_path(self, global_scope, metric_class, statement, query_count):
        query_cl = statement.consistency_level
        cl_scope = scope_for_cl(global_scope, query_cl)
        # Validate other CLs metrics are not changing. We're not messing with Quorum because of the async write coming from
        # role creation at startup.
        # If the test takes long enough, it's possible to see some of the time-based metrics shift.
        other_cls = [c for c in CL.value_to_name if c not in (query_cl, CL.QUORUM)]
        start = datetime.now()
        global_baseline = metric_class(global_scope)
        cl_baseline = metric_class(cl_scope)
        other_baselines = [metric_class(scope_for_cl(global_scope, c)) for c in other_cls]


        global_baseline.validate()
        cl_baseline.validate()
        for b in other_baselines:
            b.validate()

        for _ in range(query_count):
            self.session.execute(statement)

        global_updated = metric_class(global_scope)
        cl_updated = metric_class(cl_scope)
        end = datetime.now()
        global_updated.validate()
        cl_updated.validate()

        global_diff = global_updated.diff(global_baseline)
        cl_diff = cl_updated.diff(cl_baseline)
        for diff in [global_diff, cl_diff]:
            assert diff['TotalLatency.Count'] > 0, diff.scope
            if diff['Latency.Count'] != query_count:
                print(diff)
            assert diff['Latency.Count'] == query_count, diff.scope

        other_updated = [metric_class(scope_for_cl(global_scope, c)) for c in other_cls]
        for updated, baseline in zip(other_updated, other_baselines):
            assert updated.diff(baseline) == {}, updated.scope

        return global_diff, cl_diff

    def write_unavailables(self):
        query_cl = CL.THREE
        query_count = 5
        global_diff, _ = self.validate_query_metric('Write',
                                                    'Unavailables',
                                                    WriteMetrics,
                                                    SimpleStatement(
                                                        f"INSERT INTO {KEYSPACE}.{TABLE} (k,c) VALUES (0,0)",
                                                        consistency_level=query_cl),
                                                    query_count,
                                                    Unavailable
                                                    )
        assert global_diff['MutationSizeHistogram.Count'] == query_count

    def write_failures(self):
        query_cl = CL.ONE
        query_count = 5
        global_diff, _ = self.validate_query_metric('Write',
                                                    'Failures',
                                                    WriteMetrics,
                                                    SimpleStatement(
                                                        f"INSERT INTO {FAIL_WRITE_KEYSPACE}.{TABLE} (k,c) VALUES (0,0)",
                                                        consistency_level=query_cl),
                                                    query_count,
                                                    WriteFailure
                                                    )
        assert global_diff['MutationSizeHistogram.Count'] == query_count

    def write_timeouts(self):
        query_cl = CL.TWO
        query_count = 2
        node2 = self.cluster.nodelist()[1]
        node2.pause()
        global_diff, _ = self.validate_query_metric('Write',
                                                    "Timeouts",
                                                    WriteMetrics,
                                                    SimpleStatement(
                                                        f"INSERT INTO {KEYSPACE}.{TABLE} (k,c) VALUES (0,0)",
                                                        consistency_level=query_cl),
                                                    query_count,
                                                    WriteTimeout
                                                    )
        node2.resume()
        assert global_diff['MutationSizeHistogram.Count'] == query_count

    def read_unavailables(self):
        query_cl = CL.THREE
        self.validate_query_metric('Read',
                                   'Unavailables',
                                   RequestMetrics,
                                   SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0 AND c=0",
                                                   consistency_level=query_cl),
                                   5,
                                   Unavailable
                                   )

    def read_failures(self):
        query_cl = CL.ONE
        k = 1001

        for c in range(TOMBSTONE_FAILURE_THRESHOLD + 1):
            self.session.execute(f"DELETE FROM {KEYSPACE}.{TABLE} WHERE k={k} AND c={c}")

        self.validate_query_metric('Read',
                                   'Failures',
                                   RequestMetrics,
                                   SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k={k}",
                                                   consistency_level=query_cl),
                                   5,
                                   ReadFailure
                                   )

    def read_timeouts(self):
        query_cl = CL.TWO
        node2 = self.cluster.nodelist()[1]
        node2.pause()
        self.validate_query_metric('Read',
                                   'Timeouts',
                                   RequestMetrics,
                                   SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0",
                                                   consistency_level=query_cl),
                                   2,
                                   ReadTimeout
                                   )
        node2.resume()

    #todo: "write" only?
    def validate_query_metric(self, global_scope, meter_type, metric_class, statement, query_count, expected_exception):
        query_cl = statement.consistency_level
        cl_scope = scope_for_cl(global_scope, query_cl)

        global_baseline = metric_class(global_scope)
        cl_baseline = metric_class(cl_scope)
        cl_baseline.validate()

        exe_profile = self.session.execution_profile_clone_update(EXEC_PROFILE_DEFAULT, retry_policy=NeverRetryPolicy())
        for _ in range(query_count):
            try:
                self.session.execute(statement, execution_profile=exe_profile)
            except expected_exception:
                pass

        global_updated = metric_class(global_scope)
        global_diff = global_updated.diff(global_baseline)
        assert global_diff['Latency.Count'] == query_count
        assert global_diff[f"{meter_type}.Count"] == query_count

        cl_updated = metric_class(cl_scope)
        cl_diff = cl_updated.diff(cl_baseline)
        assert cl_diff[f"{meter_type}.Count"] == query_count
        assert cl_diff[f"{meter_type}.MeanRate"] > 0

        return global_diff, cl_diff


