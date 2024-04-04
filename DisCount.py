from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from concurrent.futures import ThreadPoolExecutor, as_completed
from cassandra.auth import PlainTextAuthProvider
from cassandra import ConsistencyLevel
import json

cloud_config= {
  'secure_connect_bundle': 'secure-connect-database.zip'
}
with open("database-token.json") as f:
    secrets = json.load(f)

CLIENT_ID = secrets["clientId"]
CLIENT_SECRET = secrets["secret"]

auth_provider = PlainTextAuthProvider(CLIENT_ID, CLIENT_SECRET)
cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
session = cluster.connect()

row = session.execute("select release_version from system.local").one()
if row:
  print(row[0])
else:
  print("An error occurred.")

class DisCount:
    def __init__(self, session, keyspace, table, consistency_level=ConsistencyLevel.LOCAL_QUORUM, num_threads=200, range_segment=200):
        self.session = session
        self.keyspace = keyspace
        self.table = table
        self.consistency_level = consistency_level
        self.num_threads = num_threads
        self.range_segment = range_segment
        self.partition_key_columns = self.get_partition_key_columns()

    def get_token_ranges(self):
        murmur3_min = -2**63
        murmur3_max = 2**63 - 1
        # Calculate the entire range divided into segments
        return self.divide_range(murmur3_min, murmur3_max)

    def get_partition_key_columns(self):
        self.session.set_keyspace('system_schema')
        query = """
            SELECT column_name
            FROM columns
            WHERE keyspace_name = %s AND table_name = %s AND kind = 'partition_key'
            ALLOW FILTERING
        """
        rows = self.session.execute(query, (self.keyspace, self.table))
        partition_key_columns = [row.column_name for row in rows]
        self.session.set_keyspace(self.keyspace)  # Switch back to the original keyspace
        return partition_key_columns

    def divide_range(self, start_token, end_token):
        """Generate token range segments lazily."""
        start_token = int(start_token)
        end_token = int(end_token)
        total_range = end_token - start_token + 1
        segment_length = max(total_range // self.range_segment, 1)  # Ensure at least 1
        
        for i in range(self.range_segment):
            segment_start = start_token + i * segment_length
            # Ensure we do not exceed the end token
            segment_end = min(segment_start + segment_length - 1, end_token)
            yield (segment_start, segment_end)
            if segment_end == end_token:
                break

    def count_rows_in_range(self, start_token, end_token):
        self.session.set_keyspace(self.keyspace)
        # Dynamically construct the token function call with partition key columns
        token_function = f"token({', '.join(self.partition_key_columns)})"
        query_str = f"SELECT count(*) FROM {self.table} WHERE {token_function} >= {start_token} AND {token_function} < {end_token}"
        query = SimpleStatement(query_str, consistency_level=self.consistency_level)
        return self.session.execute(query).one()[0]

    def count_rows(self):
        token_ranges = self.get_token_ranges()
        divided_ranges = (range for start, end in token_ranges for range in self.divide_range(start, end))

        total_count = 0
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            future_to_range = {executor.submit(self.count_rows_in_range, start, end): (start, end) for start, end in divided_ranges}
            for future in as_completed(future_to_range):
                total_count += future.result()

        return total_count

# Example usage
# Note: You need to replace 'cluster' with your actual Cassandra cluster connection object
# cluster = Cluster(['Cassandra_IP_Address'])
# discount = DisCount(cluster, 'keyspace_name', 'table_name')
# print(discount.count_rows())

discount = DisCount(session, 'keyspace', 'table')
print(discount.count_rows())
