package db

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestObfuscateQuery(t *testing.T) {
	obfuscator := NewObfuscator()

	cases := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "empty string",
			query:    "",
			expected: "",
		},
		{
			name:     "no params",
			query:    "SELECT * FROM table ORDER BY foo LIMIT 10 OFFSET 10",
			expected: "SELECT * FROM table ORDER BY foo LIMIT ? OFFSET ?",
		},

		{
			name:     "boolean",
			query:    "SELECT * FROM table where valid = true ORDER BY foo LIMIT 10 OFFSET 10",
			expected: "SELECT * FROM table where valid = true ORDER BY foo LIMIT ? OFFSET ?",
		},
		{
			name:     "comparison operators",
			query:    "SELECT * FROM table WHERE foo = 'bar' and x > 123456789 and y < 0.5 and y > 0 and z > '123' and a >= 5.53 or b <= 1 and c <> 30 and d != 'foo'",
			expected: "SELECT * FROM table WHERE foo = ? and x > ? and y < ? and y > ? and z > ? and a >= ? or b <= ? and c <> ? and d != ?",
		},
		{
			name:     "respects space",
			query:    "SELECT * FROM table WHERE foo =  'bar' and x>123456789 and y   < 0.5",
			expected: "SELECT * FROM table WHERE foo =  ? and x>? and y   < ?",
		},
		{
			name:     "logical operators",
			query:    "SELECT * FROM table WHERE (id > 1 OR other_id = 5) AND (foo <> 'bar')",
			expected: "SELECT * FROM table WHERE (id > ? OR other_id = ?) AND (foo <> ?)",
		},
		{
			name:     "like",
			query:    "SELECT * FROM table where name like 'foo' LIMIT 10",
			expected: "SELECT * FROM table where name like ? LIMIT ?",
		},
		{
			name:     "LIKE",
			query:    "SELECT * FROM table where name LIKE 'foo' LIMIT 10",
			expected: "SELECT * FROM table where name LIKE ? LIMIT ?",
		},
		{
			name:     "like %",
			query:    "SELECT * FROM table where name like '%foo' LIMIT 10",
			expected: "SELECT * FROM table where name like ? LIMIT ?",
		},
		{
			name:     "like %",
			query:    "SELECT * FROM table where name like '%foo%' LIMIT 10",
			expected: "SELECT * FROM table where name like ? LIMIT ?",
		},
		{
			name:     "ilike %",
			query:    "SELECT * FROM table where name ilike '%foo%' LIMIT 10",
			expected: "SELECT * FROM table where name ilike ? LIMIT ?",
		},
		{
			name:     "in",
			query:    "SELECT * FROM table where id in (1, 2, 3) LIMIT 10",
			expected: "SELECT * FROM table where id in (?) LIMIT ?",
		},
		{
			name:     "IN",
			query:    "SELECT * FROM table where id IN (1, 2, 3) LIMIT 10",
			expected: "SELECT * FROM table where id IN (?) LIMIT ?",
		},
		{
			name:     "in - floats + strings",
			query:    "SELECT * FROM table where id in (1.0, '2', 'foo(bar)') LIMIT 10",
			expected: "SELECT * FROM table where id in (?) LIMIT ?",
		},
		{
			name:     "string with spaces and numbers",
			query:    "SELECT * FROM table where foo = 'john123.foo@gmail.com 123' LIMIT 10",
			expected: "SELECT * FROM table where foo = ? LIMIT ?",
		},
		{
			name:     "string with escpaed quotes",
			query:    "SELECT * FROM table where foo = '\"foo\"' LIMIT 10",
			expected: "SELECT * FROM table where foo = ? LIMIT ?",
		},
		{
			name:     "quotes",
			query:    "SELECT \"users\".* FROM \"users\" WHERE (LOWER(CAST(\"users\".\"unconfirmed_email\" AS CHAR(256))) LIKE '%john.doe@gmail.com%' OR LOWER(CAST(\"users\".\"email\" AS CHAR(256))) LIKE '%john.doe@gmail.com%') ORDER BY users.created_at desc LIMIT 20 OFFSET 0",
			expected: "SELECT \"users\".* FROM \"users\" WHERE (LOWER(CAST(\"users\".\"unconfirmed_email\" AS CHAR(256))) LIKE ? OR LOWER(CAST(\"users\".\"email\" AS CHAR(256))) LIKE ?) ORDER BY users.created_at desc LIMIT ? OFFSET ?",
		},
		{
			name:     "count + datetime",
			query:    "SELECT COUNT(*) FROM \"metrics\" WHERE (measured_at > '2022-08-24 18:38:37.405596')",
			expected: "SELECT COUNT(*) FROM \"metrics\" WHERE (measured_at > ?)",
		},
		{
			name:     "joins",
			query:    "SELECT * FROM \"subscriptions\" s INNER JOIN \"plans\" p ON p.\"id\" = s.\"plan_id\" WHERE s.\"valid\" IS NOT NULL AND p.\"name\" = 'Standard' AND (s.\"subscribed_at\" >= '2020-08-14 03:06:28.376741' AND s.\"subscribed_at\" <= '2022-08-14 03:06:28.376869') ",
			expected: "SELECT * FROM \"subscriptions\" s INNER JOIN \"plans\" p ON p.\"id\" = s.\"plan_id\" WHERE s.\"valid\" IS NOT NULL AND p.\"name\" = ? AND (s.\"subscribed_at\" >= ? AND s.\"subscribed_at\" <= ?) ",
		},
		{
			name:     "joins + functions",
			query:    "SELECT COUNT(*) AS count_all, DATE_TRUNC('day', \"subscriptions\".\"subscribed_at\"::timestamptz AT TIME ZONE 'Etc/UTC') AT TIME ZONE 'Etc/UTC' AS date_trunc_day_subscriptions_subscribed_at_timestamptz_at_time_ FROM \"subscriptions\" INNER JOIN \"plans\" ON \"plans\".\"id\" = \"subscriptions\".\"plan_id\" WHERE \"subscriptions\".\"valid\" IS NOT NULL AND \"plans\".\"name\" = 'Free' AND (\"subscriptions\".\"subscribed_at\" >= '2020-08-14 03:06:28.376741' AND \"subscriptions\".\"subscribed_at\" <= '2022-08-14 03:06:28.376869') ",
			expected: "SELECT COUNT(*) AS count_all, DATE_TRUNC('day', \"subscriptions\".\"subscribed_at\"::timestamptz AT TIME ZONE 'Etc/UTC') AT TIME ZONE 'Etc/UTC' AS date_trunc_day_subscriptions_subscribed_at_timestamptz_at_time_ FROM \"subscriptions\" INNER JOIN \"plans\" ON \"plans\".\"id\" = \"subscriptions\".\"plan_id\" WHERE \"subscriptions\".\"valid\" IS NOT NULL AND \"plans\".\"name\" = ? AND (\"subscriptions\".\"subscribed_at\" >= ? AND \"subscriptions\".\"subscribed_at\" <= ?) ",
		},
		{
			name:     "with cte and window function",
			query:    "WITH recent_stats AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY measured_at DESC) AS row_number FROM stats WHERE id in (1,2,3,4,5) AND measured_at >= '2022-08-24 18:38:48.100073' AND measured_at <= '2022-08-24 18:38:48.100073') SELECT * FROM recent_stats WHERE row_number = 1",
			expected: "WITH recent_stats AS ( SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY measured_at DESC) AS row_number FROM stats WHERE id in (?) AND measured_at >= ? AND measured_at <= ?) SELECT * FROM recent_stats WHERE row_number = ?",
		},
		{
			name:     "other functions",
			query:    "SELECT LOWER(email) AS lowercase_email, time_bucket('1 hour') as time FROM \"data\" WHERE \"data\".\"valid\" = false",
			expected: "SELECT LOWER(email) AS lowercase_email, time_bucket('1 hour') as time FROM \"data\" WHERE \"data\".\"valid\" = false",
		},
		{
			name:     "aggregates",
			query:    "SELECT COUNT(*), AVG(field), MAX(field), MIN(field) FROM \"metrics\" WHERE (measured_at > '2022-08-24 18:38:37.405596')",
			expected: "SELECT COUNT(*), AVG(field), MAX(field), MIN(field) FROM \"metrics\" WHERE (measured_at > ?)",
		},
		{
			name:     "aggregates with aliases",
			query:    "SELECT COUNT(*) as c, AVG(field) AS a, MAX(field) as max, MIN(field) AS min FROM \"metrics\" WHERE (measured_at > '2022-08-24 18:38:37.405596')",
			expected: "SELECT COUNT(*) as c, AVG(field) AS a, MAX(field) as max, MIN(field) AS min FROM \"metrics\" WHERE (measured_at > ?)",
		},
		{
			name:     "extract",
			query:    "SELECT extract ('epoch' from AVG(metrics.created_at - metrics.reported_at)) as avg_write_delay from metrics",
			expected: "SELECT extract ('epoch' from AVG(metrics.created_at - metrics.reported_at)) as avg_write_delay from metrics",
		},
		{
			name:     "insert",
			query:    "INSERT INTO \"alerts\" (\"user_id\", \"created_at\", \"updated_at\", \"send_at\") VALUES (349403, '2022-08-24 18:38:48.100073', '2022-08-24 18:38:48.100073', '2022-08-24 18:38:48.098176') RETURNING \"id\"",
			expected: "INSERT INTO \"alerts\" (\"user_id\", \"created_at\", \"updated_at\", \"send_at\") VALUES (?) RETURNING \"id\"",
		},
		{
			name:     "insert single",
			query:    "INSERT INTO \"users\" (\"account_id\", \"email\", \"ssn\", \"created_at\", \"updated_at\") VALUES (1, 'john.doe@gmail.com', '123-45-6789', '2022-08-24 18:38:48.100073', '2022-08-24 18:38:48.100073') RETURNING \"id\"",
			expected: "INSERT INTO \"users\" (\"account_id\", \"email\", \"ssn\", \"created_at\", \"updated_at\") VALUES (?) RETURNING \"id\"",
		},
		{
			name:     "insert on conflict",
			query:    "INSERT INTO \"users\" (\"account_id\", \"email\", \"ssn\", \"created_at\", \"updated_at\") VALUES (1, 'john.doe@gmail.com', '123-45-6789', '2022-08-24 18:38:48.100073', '2022-08-24 18:38:48.100073') ON CONFLICT DO NOTHING RETURNING \"id\"",
			expected: "INSERT INTO \"users\" (\"account_id\", \"email\", \"ssn\", \"created_at\", \"updated_at\") VALUES (?) ON CONFLICT DO NOTHING RETURNING \"id\"",
		},
		{
			name:     "insert multiple",
			query:    "INSERT INTO \"users\" (\"account_id\", \"email\", \"ssn\", \"created_at\", \"updated_at\") VALUES (1, 'john.doe@gmail.com', '123-45-6789', '2022-08-24 18:38:48.100073', '2022-08-24 18:38:48.100073'), (2, 'john.doe2@gmail.com', '123-45-6710', '2022-08-24 18:38:48.100073', '2022-08-24 18:38:48.100073') ON CONFLICT DO NOTHING RETURNING \"id\"",
			expected: "INSERT INTO \"users\" (\"account_id\", \"email\", \"ssn\", \"created_at\", \"updated_at\") VALUES (?) ON CONFLICT DO NOTHING RETURNING \"id\"",
		},
		{
			name:     "update",
			query:    "UPDATE \"users\" SET \"updated_at\" = '2022-08-24 19:44:58.241265', \"priority\" = '2', \"missing_query_text\" = TRUE WHERE \"users\".\"id\" = 1",
			expected: "UPDATE \"users\" SET \"updated_at\" = ?, \"priority\" = ?, \"missing_query_text\" = TRUE WHERE \"users\".\"id\" = ?",
		},
		{
			name:     "delete",
			query:    "DELETE FROM \"users\" WHERE (created_at > '2022-08-24 19:56:34.587818' AND id in (1,2,3) LIMIT 10)",
			expected: "DELETE FROM \"users\" WHERE (created_at > ? AND id in (?) LIMIT ?)",
		},
		{
			name:     "between",
			query:    "SELECT * FROM table where foo between '2022-08-23 19:56:34.587818' AND '2022-08-24 19:56:34.587818'",
			expected: "SELECT * FROM table where foo between ? AND ?",
		},
		{
			name:     "between symmetric",
			query:    "SELECT * FROM table where foo between symmetric 1000 AND 2000",
			expected: "SELECT * FROM table where foo between symmetric ? AND ?",
		},
		{
			name:     "create table",
			query:    "CREATE TABLE \"tags\" (\"id\" bigserial primary key, \"account_id\" bigint NOT NULL, \"user_id\" bigint NOT NULL, \"category\" character varying NOT NULL, \"text\" character varying NOT NULL, \"created_at\" timestamp(6) NOT NULL, \"updated_at\" timestamp(6) NOT NULL)",
			expected: "CREATE TABLE \"tags\" (\"id\" bigserial primary key, \"account_id\" bigint NOT NULL, \"user_id\" bigint NOT NULL, \"category\" character varying NOT NULL, \"text\" character varying NOT NULL, \"created_at\" timestamp(6) NOT NULL, \"updated_at\" timestamp(6) NOT NULL)",
		},
		{
			name:     "alter table",
			query:    "ALTER TABLE \"tags\" ADD \"other_value\" character varying",
			expected: "ALTER TABLE \"tags\" ADD \"other_value\" character varying",
		},
		{
			name:     "alter column",
			query:    "ALTER TABLE \"tags\" ALTER COLUMN \"other_value\" SET NOT NULL",
			expected: "ALTER TABLE \"tags\" ALTER COLUMN \"other_value\" SET NOT NULL",
		},
		{
			name:     "create index",
			query:    "CREATE INDEX \"index_tags_on_account_id\" ON \"tags\" (\"account_id\")",
			expected: "CREATE INDEX \"index_tags_on_account_id\" ON \"tags\" (\"account_id\")",
		},
		{
			name:     "create unique index",
			query:    "CREATE UNIQUE INDEX \"index_tags_on_text\" ON \"tags\" (\"text\")",
			expected: "CREATE UNIQUE INDEX \"index_tags_on_text\" ON \"tags\" (\"text\")",
		},
		{
			name:     "comparison operators - bind params",
			query:    "SELECT * FROM table WHERE foo = $1 and x > $2 and y < $3 and y > $4 and z > $5 and a >= $6 or b <= $7 and c <> $8 and d != $9",
			expected: "SELECT * FROM table WHERE foo = ? and x > ? and y < ? and y > ? and z > ? and a >= ? or b <= ? and c <> ? and d != ?",
		},
		{
			name:     "like - bind params",
			query:    "SELECT * FROM table where name like $1 LIMIT 10",
			expected: "SELECT * FROM table where name like ? LIMIT ?",
		},
		{
			name:     "ilike % - bind params",
			query:    "SELECT * FROM table where name ilike $1 LIMIT 10",
			expected: "SELECT * FROM table where name ilike ? LIMIT ?",
		},
		{
			name:     "in - bind params",
			query:    "SELECT * FROM table where id in ($1, $2, $3) LIMIT 10",
			expected: "SELECT * FROM table where id in (?) LIMIT ?",
		},
		{
			name:     "between - bind params",
			query:    "SELECT * FROM table where foo between $1 AND $2",
			expected: "SELECT * FROM table where foo between ? AND ?",
		},
		{
			name:     "select and limit and offset - bind params",
			query:    "SELECT $1 AS one FROM \"alerts\" WHERE \"alerts\".\"condition_id\" = $2 AND (alerted_at > $3) LIMIT $4 OFFSET $5",
			expected: "SELECT ? AS one FROM \"alerts\" WHERE \"alerts\".\"condition_id\" = ? AND (alerted_at > ?) LIMIT ? OFFSET ?",
		},
		{
			name:     "case when then",
			query:    "SELECT name, value, CASE WHEN value = '1' THEN 'yes' ELSE NULL END AS value_foo FROM data",
			expected: "SELECT name, value, CASE WHEN value = ? THEN ? ELSE NULL END AS value_foo FROM data",
		},
		{
			name:     "case when then - bind params",
			query:    "SELECT name, value, CASE WHEN value = $1 THEN $2 ELSE NULL END AS value_foo FROM data",
			expected: "SELECT name, value, CASE WHEN value = ? THEN ? ELSE NULL END AS value_foo FROM data",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, obfuscator.ObfuscateQuery(c.query))
		})
	}
}

func TestObfuscateExplain(t *testing.T) {
	obfuscator := NewObfuscator()

	cases := []struct {
		name     string
		explain  string
		expected string
	}{
		{
			name:     "empty string",
			explain:  "",
			expected: "",
		},
		{
			name: "no params",
			explain: `Finalize Aggregate  (cost=555558.15..555558.15 rows=1 width=8)
			->  Gather  (cost=555557.94..555558.15 rows=2 width=8)
						Workers Planned: 2
						->  Partial Aggregate  (cost=554557.94..554557.95 rows=1 width=8)
									->  Parallel Index Only Scan using index_users_on_created_at on users  (cost=0.11..539895.43 rows=29325031 width=0)`,
			expected: `Finalize Aggregate  (cost=555558.15..555558.15 rows=1 width=8)
			->  Gather  (cost=555557.94..555558.15 rows=2 width=8)
						Workers Planned: 2
						->  Partial Aggregate  (cost=554557.94..554557.95 rows=1 width=8)
									->  Parallel Index Only Scan using index_users_on_created_at on users  (cost=0.11..539895.43 rows=29325031 width=0)`,
		},
		{
			name: "= operator",
			explain: `Limit  (cost=0.09..4.09 rows=1 width=247)
			->  LockRows  (cost=0.09..4.09 rows=1 width=247)
						->  Index Scan using users_pkey on users  (cost=0.09..4.09 rows=1 width=247)
									Index Cond: (id = 1181715)`,
			expected: `Limit  (cost=0.09..4.09 rows=1 width=247)
			->  LockRows  (cost=0.09..4.09 rows=1 width=247)
						->  Index Scan using users_pkey on users  (cost=0.09..4.09 rows=1 width=247)
									Index Cond: (id = ?)`,
		},
		{
			name: "multiple operators",
			explain: `GroupAggregate  (cost=0.11..24876.44 rows=13 width=15)
			Group Key: name
			->  Index Only Scan using index_users_on_name_and_created_at on users  (cost=0.11..24876.33 rows=69 width=7)
						Index Cond: ((name = 'John'::text) AND (created_at >= '2022-08-27 11:31:06.910708'::timestamp without time zone))`,
			expected: `GroupAggregate  (cost=0.11..24876.44 rows=13 width=15)
			Group Key: name
			->  Index Only Scan using index_users_on_name_and_created_at on users  (cost=0.11..24876.33 rows=69 width=7)
						Index Cond: ((name = ?::text) AND (created_at >= ?::timestamp without time zone))`,
		},
		{
			name: "filter",
			explain: `Update on channels  (cost=1652.12..8854.01 rows=2000 width=270)
			->  Nested Loop  (cost=1652.12..8854.01 rows=2000 width=270)
						->  HashAggregate  (cost=1652.04..1658.04 rows=2000 width=40)
									Group Key: "ANY_subquery".id
									->  Subquery Scan on "ANY_subquery"  (cost=0.00..1651.04 rows=2000 width=40)
												->  Limit  (cost=0.00..1645.04 rows=2000 width=8)
															->  Seq Scan on channels channels_1  (cost=0.00..17170.06 rows=20875 width=8)
																		Filter: ((reset_at < '2022-08-29 00:00:00'::timestamp without time zone) AND ((type)::text = 'PhoneChannel'::text))
						->  Index Scan using channels_pkey on channels  (cost=0.08..3.61 rows=1 width=226)
									Index Cond: (id = "ANY_subquery".id)`,
			expected: `Update on channels  (cost=1652.12..8854.01 rows=2000 width=270)
			->  Nested Loop  (cost=1652.12..8854.01 rows=2000 width=270)
						->  HashAggregate  (cost=1652.04..1658.04 rows=2000 width=40)
									Group Key: "ANY_subquery".id
									->  Subquery Scan on "ANY_subquery"  (cost=0.00..1651.04 rows=2000 width=40)
												->  Limit  (cost=0.00..1645.04 rows=2000 width=8)
															->  Seq Scan on channels channels_1  (cost=0.00..17170.06 rows=20875 width=8)
																		Filter: ((reset_at < ?::timestamp without time zone) AND ((type)::text = ?::text))
						->  Index Scan using channels_pkey on channels  (cost=0.08..3.61 rows=1 width=226)
									Index Cond: (id = "ANY_subquery".id)`,
		},
		{
			name: "multiple filters",
			explain: `Hash Join  (cost=1.37..1642.54 rows=10620 width=159)
			Hash Cond: (subscriptions.plan_id = plans.id)
			->  Merge Join  (cost=0.32..1625.72 rows=16992 width=159)
						Merge Cond: (subscriptions.payment_method_id = payment_methods.id)
						->  Index Scan using index_subscriptions_type_payment_method_id_subscribed_at on subscriptions  (cost=0.08..26007.81 rows=416133 width=159)
									Index Cond: ((type)::text = 'Paid'::text)
									Filter: ((ended_at IS NULL) OR (ended_at > '2022-08-29 17:10:41.759895'::timestamp without time zone))
						->  Index Scan using payment_methods_pkey on payment_methods  (cost=0.06..446.98 rows=12992 width=8)
									Filter: ((type)::text = 'CreditCard'::text)
			->  Hash  (cost=1.03..1.03 rows=5 width=8)
						->  Seq Scan on plans  (cost=0.00..1.03 rows=5 width=8)
									Filter: ((price > 0.0) OR ((name)::text = 'Paid'::text))`,
			expected: `Hash Join  (cost=1.37..1642.54 rows=10620 width=159)
			Hash Cond: (subscriptions.plan_id = plans.id)
			->  Merge Join  (cost=0.32..1625.72 rows=16992 width=159)
						Merge Cond: (subscriptions.payment_method_id = payment_methods.id)
						->  Index Scan using index_subscriptions_type_payment_method_id_subscribed_at on subscriptions  (cost=0.08..26007.81 rows=416133 width=159)
									Index Cond: ((type)::text = ?::text)
									Filter: ((ended_at IS NULL) OR (ended_at > ?::timestamp without time zone))
						->  Index Scan using payment_methods_pkey on payment_methods  (cost=0.06..446.98 rows=12992 width=8)
									Filter: ((type)::text = ?::text)
			->  Hash  (cost=1.03..1.03 rows=5 width=8)
						->  Seq Scan on plans  (cost=0.00..1.03 rows=5 width=8)
									Filter: ((price > ?) OR ((name)::text = ?::text))`,
		},
		{
			name: "multiple index cond",
			explain: `Limit  (cost=37744.68..37848.36 rows=969 width=8)
			->  Gather Merge  (cost=37744.68..37848.36 rows=969 width=8)
						Workers Planned: 1
						->  Sort  (cost=36744.68..36745.16 rows=969 width=8)
									Sort Key: users.id
									->  Nested Loop  (cost=1929.44..36735.07 rows=969 width=8)
												->  Parallel Bitmap Heap Scan on users  (cost=1929.35..34250.57 rows=1442 width=16)
															Recheck Cond: ((((type)::text = 'User'::text) AND ((name)::text = 'John'::text) AND (priority = 3) AND enabled AND (reset_at IS NULL)) OR (((type)::text = 'User'::text) AND ((name)::text = 'John'::text) AND (priority = 3) AND enabled AND (reset_at < '2022-08-29 18:32:35.012758'::timestamp without time zone)))
															Filter: (enabled AND ((friend_id = 86) OR (friend_id IS NULL)))
															->  BitmapOr  (cost=1929.35..1929.35 rows=36825 width=0)
																		->  Bitmap Index Scan on idx_users_type_name_priority_enabled_reset_at  (cost=0.00..1121.01 rows=21401 width=0)
																					Index Cond: (((type)::text = 'User'::text) AND ((name)::text = 'John'::text) AND (priority = 3) AND (enabled = true) AND (reset_at IS NULL))
																		->  Bitmap Index Scan on idx_alert_conditions_type_name_priority_enabled_cooldown_till  (cost=0.00..808.09 rows=15424 width=0)
																					Index Cond: (((type)::text = 'User'::text) AND ((name)::text = 'John'::text) AND (priority = 3) AND (enabled = true) AND (reset_at < '2022-08-29 18:32:35.012758'::timestamp without time zone))
												->  Index Only Scan using index_channels_on_id_and_confirmed_at on channels  (cost=0.08..1.72 rows=1 width=8)
															Index Cond: ((id = users.channel_id) AND (confirmed_at IS NOT NULL))`,
			expected: `Limit  (cost=37744.68..37848.36 rows=969 width=8)
			->  Gather Merge  (cost=37744.68..37848.36 rows=969 width=8)
						Workers Planned: 1
						->  Sort  (cost=36744.68..36745.16 rows=969 width=8)
									Sort Key: users.id
									->  Nested Loop  (cost=1929.44..36735.07 rows=969 width=8)
												->  Parallel Bitmap Heap Scan on users  (cost=1929.35..34250.57 rows=1442 width=16)
															Recheck Cond: ((((type)::text = ?::text) AND ((name)::text = ?::text) AND (priority = ?) AND enabled AND (reset_at IS NULL)) OR (((type)::text = ?::text) AND ((name)::text = ?::text) AND (priority = ?) AND enabled AND (reset_at < ?::timestamp without time zone)))
															Filter: (enabled AND ((friend_id = ?) OR (friend_id IS NULL)))
															->  BitmapOr  (cost=1929.35..1929.35 rows=36825 width=0)
																		->  Bitmap Index Scan on idx_users_type_name_priority_enabled_reset_at  (cost=0.00..1121.01 rows=21401 width=0)
																					Index Cond: (((type)::text = ?::text) AND ((name)::text = ?::text) AND (priority = ?) AND (enabled = true) AND (reset_at IS NULL))
																		->  Bitmap Index Scan on idx_alert_conditions_type_name_priority_enabled_cooldown_till  (cost=0.00..808.09 rows=15424 width=0)
																					Index Cond: (((type)::text = ?::text) AND ((name)::text = ?::text) AND (priority = ?) AND (enabled = true) AND (reset_at < ?::timestamp without time zone))
												->  Index Only Scan using index_channels_on_id_and_confirmed_at on channels  (cost=0.08..1.72 rows=1 width=8)
															Index Cond: ((id = users.channel_id) AND (confirmed_at IS NOT NULL))`,
		},
		{
			name: "recheck cond",
			explain: `Delete on metrics  (cost=14734.05..364534.16 rows=1855459 width=6)
			->  Bitmap Heap Scan on metrics  (cost=14734.05..364534.16 rows=1855459 width=6)
						Recheck Cond: (created_at < '2022-02-28 00:47:01.638879'::timestamp without time zone)
						->  Bitmap Index Scan on index_metrics_on_created_at  (cost=0.00..14641.28 rows=1855459 width=0)
									Index Cond: (created_at < '2022-02-28 00:47:01.638879'::timestamp without time zone)`,
			expected: `Delete on metrics  (cost=14734.05..364534.16 rows=1855459 width=6)
			->  Bitmap Heap Scan on metrics  (cost=14734.05..364534.16 rows=1855459 width=6)
						Recheck Cond: (created_at < ?::timestamp without time zone)
						->  Bitmap Index Scan on index_metrics_on_created_at  (cost=0.00..14641.28 rows=1855459 width=0)
									Index Cond: (created_at < ?::timestamp without time zone)`,
		},
		{
			name: "count(*) >",
			explain: `HashAggregate  (cost=38472.64..38506.44 rows=11265 width=16)
			Group Key: user_id
			Filter: (count(*) > 5)
			->  Index Scan using index_actions_on_created_at_and_enabled on actions  (cost=0.11..38412.17 rows=40317 width=8)
						Index Cond: ((created_at >= '2022-08-01 00:00:00'::timestamp without time zone) AND (created_at < '2022-08-31 23:59:59.999999'::timestamp without time zone) AND (enabled = true))
						Filter: enabled`,
			expected: `HashAggregate  (cost=38472.64..38506.44 rows=11265 width=16)
			Group Key: user_id
			Filter: (count(*) > ?)
			->  Index Scan using index_actions_on_created_at_and_enabled on actions  (cost=0.11..38412.17 rows=40317 width=8)
						Index Cond: ((created_at >= ?::timestamp without time zone) AND (created_at < ?::timestamp without time zone) AND (enabled = true))
						Filter: enabled`,
		},
		{
			name: "~~ operator",
			explain: `Limit  (cost=0.08..35910.12 rows=20 width=1281)
			->  Index Scan Backward using index_users_on_created_at on users  (cost=0.08..147231.24 rows=82 width=1281)
						Filter: ((lower(((email)::character(256))::text) ~~ '%john.doe@gmail%'::text) OR (lower(((email)::character(256))::text) ~~ '%jane.doe%'::text))`,
			expected: `Limit  (cost=0.08..35910.12 rows=20 width=1281)
			->  Index Scan Backward using index_users_on_created_at on users  (cost=0.08..147231.24 rows=82 width=1281)
						Filter: ((lower(((email)::character(256))::text) ~~ ?::text) OR (lower(((email)::character(256))::text) ~~ ?::text))`,
		},
		{
			name: "deep plan",
			explain: `GroupAggregate  (cost=16871.89..16872.90 rows=202 width=16)
			Group Key: users.id
			Filter: (count(notifications.id) >= 100)
			->  Sort  (cost=16871.89..16871.99 rows=202 width=16)
						Sort Key: users.id
						->  Nested Loop  (cost=15253.52..16870.34 rows=202 width=16)
									->  Hash Join  (cost=15253.43..16849.42 rows=223 width=32)
												Hash Cond: (subscriptions.plan_id = plans.id)
												->  Nested Loop  (cost=15251.33..16846.38 rows=356 width=40)
															->  Nested Loop  (cost=15251.25..16791.45 rows=349 width=24)
																		->  Hash Join  (cost=15251.17..15974.92 rows=349 width=16)
																					Hash Cond: (notifications.foo_id = foo.id)
																					->  Index Scan using index_notifications_on_delivered_at on notifications  (cost=0.11..719.09 rows=9097 width=24)
																								Index Cond: ((delivered_at IS NOT NULL) AND (delivered_at > '2022-09-01 00:00:00'::timestamp without time zone))
																					->  Hash  (cost=15153.28..15153.28 rows=27934 width=8)
																								->  Bitmap Heap Scan on foo  (cost=819.38..15153.28 rows=27934 width=8)
																											Recheck Cond: ((type)::text = 'Push'::text)
																											->  Bitmap Index Scan on idx_foo_delivered_at  (cost=0.00..817.99 rows=27934 width=0)
																														Index Cond: ((type)::text = 'Push'::text)
																		->  Index Scan using users_pkey on users  (cost=0.08..2.34 rows=1 width=16)
																					Index Cond: (id = notifications.user_id)
															->  Index Scan using index_subscriptions_on_account_id on subscriptions  (cost=0.08..0.15 rows=1 width=16)
																		Index Cond: (account_id = users.account_id)
																		Filter: (((ended_at IS NULL) OR (ended_at > '2022-09-01 06:47:07.090132'::timestamp without time zone)) AND ((type)::text = 'Paid'::text))
												->  Hash  (cost=2.08..2.08 rows=5 width=16)
															->  Hash Join  (cost=1.05..2.08 rows=5 width=16)
																		Hash Cond: (plans_subscriptions.id = plans.id)
																		->  Seq Scan on plans plans_subscriptions  (cost=0.00..1.02 rows=8 width=8)
																		->  Hash  (cost=1.03..1.03 rows=5 width=8)
																					->  Seq Scan on plans  (cost=0.00..1.03 rows=5 width=8)
																								Filter: ((price > 0.0) OR ((name)::text = 'Paid'::text))
									->  Index Only Scan using accounts_pkey on accounts  (cost=0.08..0.09 rows=1 width=8)
												Index Cond: (id = users.account_id)`,
			expected: `GroupAggregate  (cost=16871.89..16872.90 rows=202 width=16)
			Group Key: users.id
			Filter: (count(notifications.id) >= ?)
			->  Sort  (cost=16871.89..16871.99 rows=202 width=16)
						Sort Key: users.id
						->  Nested Loop  (cost=15253.52..16870.34 rows=202 width=16)
									->  Hash Join  (cost=15253.43..16849.42 rows=223 width=32)
												Hash Cond: (subscriptions.plan_id = plans.id)
												->  Nested Loop  (cost=15251.33..16846.38 rows=356 width=40)
															->  Nested Loop  (cost=15251.25..16791.45 rows=349 width=24)
																		->  Hash Join  (cost=15251.17..15974.92 rows=349 width=16)
																					Hash Cond: (notifications.foo_id = foo.id)
																					->  Index Scan using index_notifications_on_delivered_at on notifications  (cost=0.11..719.09 rows=9097 width=24)
																								Index Cond: ((delivered_at IS NOT NULL) AND (delivered_at > ?::timestamp without time zone))
																					->  Hash  (cost=15153.28..15153.28 rows=27934 width=8)
																								->  Bitmap Heap Scan on foo  (cost=819.38..15153.28 rows=27934 width=8)
																											Recheck Cond: ((type)::text = ?::text)
																											->  Bitmap Index Scan on idx_foo_delivered_at  (cost=0.00..817.99 rows=27934 width=0)
																														Index Cond: ((type)::text = ?::text)
																		->  Index Scan using users_pkey on users  (cost=0.08..2.34 rows=1 width=16)
																					Index Cond: (id = notifications.user_id)
															->  Index Scan using index_subscriptions_on_account_id on subscriptions  (cost=0.08..0.15 rows=1 width=16)
																		Index Cond: (account_id = users.account_id)
																		Filter: (((ended_at IS NULL) OR (ended_at > ?::timestamp without time zone)) AND ((type)::text = ?::text))
												->  Hash  (cost=2.08..2.08 rows=5 width=16)
															->  Hash Join  (cost=1.05..2.08 rows=5 width=16)
																		Hash Cond: (plans_subscriptions.id = plans.id)
																		->  Seq Scan on plans plans_subscriptions  (cost=0.00..1.02 rows=8 width=8)
																		->  Hash  (cost=1.03..1.03 rows=5 width=8)
																					->  Seq Scan on plans  (cost=0.00..1.03 rows=5 width=8)
																								Filter: ((price > ?) OR ((name)::text = ?::text))
									->  Index Only Scan using accounts_pkey on accounts  (cost=0.08..0.09 rows=1 width=8)
												Index Cond: (id = users.account_id)`,
		},
		{
			name: "any list",
			explain: `Gather  (cost=1060.61..7509.95 rows=927 width=325)
			Workers Planned: 1
			->  Nested Loop  (cost=60.61..6417.25 rows=545 width=325)
						->  Parallel Bitmap Heap Scan on users  (cost=58.76..1161.69 rows=1364 width=116)
									Recheck Cond: (id = ANY ('{72,18,82,75,8}'::bigint[]))
									Filter: (enabled AND (friend_id = ANY ('{1,33,10,2,37,9679}'::bigint[])))
									->  Bitmap Index Scan on index_users_on_friend_id_and_online  (cost=0.00..58.65 rows=4116 width=0)
												Index Cond: ((friend_id = ANY ('{72,18,82,75,8}'::bigint[])) AND (online = true))
						->  Bitmap Heap Scan on user_stats  (cost=1.85..3.85 rows=1 width=209)
									Recheck Cond: (user_id = users.id)
									Filter: (last_paid > 0.1)
									->  Bitmap Index Scan on index_user_stats_on_friend_id  (cost=0.00..1.85 rows=1 width=0)
												Index Cond: (user_id = users.id)`,
			expected: `Gather  (cost=1060.61..7509.95 rows=927 width=325)
			Workers Planned: 1
			->  Nested Loop  (cost=60.61..6417.25 rows=545 width=325)
						->  Parallel Bitmap Heap Scan on users  (cost=58.76..1161.69 rows=1364 width=116)
									Recheck Cond: (id = ANY (?::bigint[]))
									Filter: (enabled AND (friend_id = ANY (?::bigint[])))
									->  Bitmap Index Scan on index_users_on_friend_id_and_online  (cost=0.00..58.65 rows=4116 width=0)
												Index Cond: ((friend_id = ANY (?::bigint[])) AND (online = true))
						->  Bitmap Heap Scan on user_stats  (cost=1.85..3.85 rows=1 width=209)
									Recheck Cond: (user_id = users.id)
									Filter: (last_paid > ?)
									->  Bitmap Index Scan on index_user_stats_on_friend_id  (cost=0.00..1.85 rows=1 width=0)
												Index Cond: (user_id = users.id)`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.expected, obfuscator.ObfuscateExplain(c.explain))
		})
	}
}
