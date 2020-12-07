import pytest
from cassandra.protocol import ServerError

from dtest import Tester, create_ks

from cassandra import (ReadFailure,
                       ConsistencyLevel as CL)
from cassandra.policies import FallthroughRetryPolicy
from cassandra.query import SimpleStatement

since = pytest.mark.since

KEYSPACE = 'ks'
TABLE = 't'
TOMBSTONE_FAILURE_THRESHOLD = 20
TOMBSTONE_FAIL_KEY = 10000001


class NoException(BaseException):
    pass


@since('4.0')
class TestDigestResolverAssertionError(Tester):

    @pytest.fixture(autouse=True)
    def fixture_add_additional_log_patterns(self, fixture_dtest_setup):
        fixture_dtest_setup.ignore_log_patterns = (
            f"Scanned over {TOMBSTONE_FAILURE_THRESHOLD + 1} tombstones during query"  # Caused by the read failure tests
        )

    def setup(self):
        cluster = self.cluster
        cluster.set_configuration_options({'read_request_timeout_in_ms': 30000,
                                           'write_request_timeout_in_ms': 3000,
                                           'phi_convict_threshold': 12,
                                           'tombstone_failure_threshold': TOMBSTONE_FAILURE_THRESHOLD})

        cluster.populate(2, debug=True)
        cluster.start()
        node1, node2 = cluster.nodelist()

        s = self.session = self.patient_exclusive_cql_connection(node1, retry_policy=FallthroughRetryPolicy(), request_timeout=30000000000)
        create_ks(s, KEYSPACE, 2)
        s.execute(f"CREATE TABLE {KEYSPACE}.{TABLE} (k int, c int, v int, PRIMARY KEY (k,c))")

        # node2.stop()

        # Here we're doing a series of deletions in order to create enough tombstones to exceed the configured fail threshold.
        # This partition will be used to test read failures.
        for c in range(TOMBSTONE_FAILURE_THRESHOLD + 1):
            self.session.execute(f"DELETE FROM {KEYSPACE}.{TABLE} WHERE k={TOMBSTONE_FAIL_KEY} AND c={c}")
        # node2.start()

    def test_elicit_digest_resolver_error(self):
        statement = SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k={TOMBSTONE_FAIL_KEY}",
                                    consistency_level=CL.ONE)
        self.session.execute(SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0",
                                             consistency_level=CL.ONE))
        self.session.execute(SimpleStatement(f"SELECT k FROM {KEYSPACE}.{TABLE} WHERE k=0",
                                             consistency_level=CL.TWO))
        for x in range(5000000):
            try:
                print(x)
                statement = SimpleStatement(f"SELECT k, currentTime() FROM {KEYSPACE}.{TABLE} WHERE k={TOMBSTONE_FAIL_KEY}",
                                            consistency_level=CL.ONE)
                self.session.execute(statement)
            except (ReadFailure, ServerError) as e:
                pass
            except Exception as e:
                print(x)
                raise e
        print(x)
