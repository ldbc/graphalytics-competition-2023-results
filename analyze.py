import json
import glob
import duckdb

con = duckdb.connect("gx-results.duckdb")
con.sql("""CREATE OR REPLACE TABLE timeouts AS SELECT size, timeout_minutes, timeout_minutes * 60 AS timeout_seconds FROM read_csv_auto('timeouts.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE algorithms AS FROM read_csv_auto('algorithms.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE datasets AS FROM read_csv_auto('datasets.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE platforms AS FROM read_csv_auto('platforms.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE size_ordering AS FROM read_csv_auto('size_ordering.csv');""")
con.sql("""
    CREATE OR REPLACE TABLE results (
        platform VARCHAR,
        job VARCHAR,
        algorithm VARCHAR,
        dataset VARCHAR,
        run VARCHAR,
        success BOOLEAN,
        load_time FLOAT,
        makespan FLOAT,
        processing_time FLOAT
    );
    """)

for path in glob.glob("submissions/**/*.json", recursive=True):
    with open(path) as f:
        j = json.load(f)
        platform = j["system"]["platform"]["name"]
        pricing = j["system"]["pricing"]
        environment_name = j["system"]["environment"]["name"]

        print(f"{platform}: ${pricing}, env name {environment_name}")

        for job_id in j["result"]["jobs"]:
            job = j["result"]["jobs"][job_id]
            algorithm = job["algorithm"]
            dataset = job["dataset"]
            runs = job["runs"]

            for run_id in runs:
                run = j["result"]["runs"][run_id]

                success = run["success"]
                load_time = run["load_time"]
                makespan = run["makespan"]
                processing_time = run["processing_time"]

                if platform == "GraphBLAS":
                    platform = "GraphBLAS Intel Xeon Platinum 8369"

                con.sql(f"INSERT INTO results VALUES ('{platform}', '{job_id}', '{algorithm}', '{dataset}', '{run_id}', {success}, {load_time}, {makespan}, {processing_time});")

#con.sql("""COPY results TO 'results.csv';""")

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT * EXCLUDE name
        FROM results
        JOIN datasets ON datasets.name = results.dataset
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT *
        FROM results
        WHERE success = true
          AND size IN ('S', 'M', 'L', 'XL', '2XL+')
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT platform, algorithm, dataset, size, mean(processing_time) AS time
        FROM results
        GROUP BY platform, algorithm, dataset, size
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE workload AS
        SELECT datasets.name AS dataset, datasets.size AS size, algorithms.name AS algorithm, platforms.name AS platform
        FROM datasets
        CROSS JOIN algorithms
        CROSS JOIN platforms
        WHERE (NOT algorithms.requires_weights OR datasets.weighted = true)
    ;
    """)

# for systems not having a result (time == NULL), we assign 3 times the timeout as penalty
con.sql("""
    CREATE OR REPLACE TABLE results_full AS
        SELECT workload.size AS size, workload.dataset AS dataset, workload.algorithm AS algorithm, workload.platform platform, coalesce(results.time, 3 * timeouts.timeout_seconds) AS time, platforms.pricing AS pricing
        FROM workload
        LEFT JOIN results
               ON workload.dataset = results.dataset
              AND workload.algorithm = results.algorithm
              AND workload.platform = results.platform
        JOIN timeouts
          ON timeouts.size = workload.size
        JOIN platforms
          ON platforms.name = workload.platform
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_platform_algorithm AS
        SELECT size, platform, algorithm, pricing, mean(time) AS time
        FROM results_full
        GROUP BY ALL
        ORDER BY size ASC, algorithm ASC, time ASC
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_platform AS
        SELECT
            size,
            platform,
            mean(time) AS time,
            pricing,
        FROM results_full
        GROUP BY ALL
        ORDER BY size ASC, time ASC
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_platform_price_adjusted AS
        SELECT
            rank,
            size,
            platform,
            time,
            pricing,
            1 / time / pricing * 1000 * 1000 * 1000 AS throughput_per_dollar
        FROM results_platform
        JOIN size_ordering
          ON size_ordering.name = results_platform.size
        ORDER BY rank ASC, throughput_per_dollar DESC
    ;
    """)

con.sql("""
    COPY (SELECT * EXCLUDE rank FROM results_platform_price_adjusted) TO 'results_platform_price_adjusted.csv' (SEPARATOR '\t');
    """)

con.sql("""
    COPY (SELECT * EXCLUDE rank FROM results_platform_price_adjusted WHERE platform != 'GraphBLAS Intel Xeon Platinum 8369') TO 'results_platform_price_adjusted_3runs.csv' (SEPARATOR '\t');
    """)
