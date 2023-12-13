import json
import glob
import duckdb

con = duckdb.connect("gx-results.duckdb")
con.sql("""CREATE OR REPLACE TABLE timeouts AS SELECT size, timeout_minutes, timeout_minutes * 60 AS timeout_seconds FROM read_csv_auto('timeouts.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE algorithms AS FROM read_csv_auto('algorithms.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE datasets AS FROM read_csv_auto('datasets.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE size_ordering AS FROM read_csv_auto('size_ordering.csv');""")
con.sql("""
    CREATE OR REPLACE TABLE results_raw (
        platform VARCHAR,
        environment_name VARCHAR,
        pricing DOUBLE,
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

        if platform == 'GraphBLAS Intel Xeon Gold 6342':
            pricing = 15354.81
        if environment_name == 'ecs.c8i.24xlarge':
            pricing = 100_000_000.00

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

                con.sql(f"INSERT INTO results_raw VALUES ('{platform}', '{environment_name}', {pricing}, '{job_id}', '{algorithm}', '{dataset}', '{run_id}', {success}, {load_time}, {makespan}, {processing_time});")

#con.sql("""COPY results TO 'results.csv';""")

con.sql("""
    CREATE OR REPLACE TABLE results_raw AS
        SELECT *
        FROM results_raw
        JOIN datasets ON datasets.name = results_raw.dataset
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT * EXCLUDE name
        FROM results_raw
        WHERE success = true
          AND size IN ('S', 'M', 'L', 'XL', '2XL+')
    ;
    """)

# load results

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT
            platform,
            environment_name,
            pricing,
            algorithm,
            dataset,
            size,
            avg(makespan) AS makespan,
            avg(processing_time) AS processing_time,
            count(*) AS runs
        FROM results
        GROUP BY ALL
        --HAVING runs >= 3
    ;
    """)

# create list of unique platform names

con.sql("""
    CREATE OR REPLACE TABLE platforms AS
        SELECT DISTINCT platform AS name
        FROM results
    ;
    """)

# create the required workload for each size category

con.sql("""
    CREATE OR REPLACE TABLE workload AS
        SELECT datasets.name AS dataset, datasets.size AS size, algorithms.name AS algorithm, platforms.name AS platform
        FROM datasets
        CROSS JOIN algorithms
        CROSS JOIN platforms
        WHERE (NOT algorithms.requires_weights OR datasets.weighted = true)
    ;
    """)

# for systems that do not have a result (time == NULL) for a given workload (=algorithm/dataset combination),
# we assign 3 times the timeout as penalty

con.sql("""
    CREATE OR REPLACE TABLE results_full AS
        SELECT
            workload.size AS size,
            workload.platform AS platform,
            results.environment_name AS environment_name,
            results.pricing AS pricing,
            workload.dataset AS dataset,
            workload.algorithm AS algorithm,
            coalesce(results.makespan, 3 * timeouts.timeout_seconds) AS makespan,
            coalesce(results.processing_time, 3 * timeouts.timeout_seconds) AS processing_time,
            coalesce(runs, 3) AS runs
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
        SELECT
            size,
            platform,
            environment_name,
            max(pricing) AS pricing,
            algorithm,
            avg(makespan) AS mean_makespan,
            avg(processing_time) AS mean_processing_time,
            min(runs) AS min_runs_per_workload_item
        FROM results_full
        GROUP BY ALL
        ORDER BY size ASC, algorithm ASC, mean_processing_time ASC
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_platform AS
        SELECT
            size,
            platform,
            environment_name,
            max(pricing) AS pricing, -- NULL prices should be consumed by max()
            avg(mean_makespan) AS mean_makespan,
            avg(mean_processing_time) AS mean_processing_time,
            min(min_runs_per_workload_item) AS min_runs_per_workload_item
        FROM results_platform_algorithm
        GROUP BY ALL
        HAVING pricing IS NOT NULL
        ORDER BY size ASC, mean_processing_time ASC
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_platform_price_adjusted AS
        SELECT
            rank,
            size,
            platform,
            environment_name,
            pricing,
            mean_makespan,
            mean_processing_time,
            1 / mean_processing_time / pricing * 1000 * 1000 * 1000 AS throughput_per_dollar,
            min(min_runs_per_workload_item) AS min_runs_per_workload_item
        FROM results_platform
        JOIN size_ordering
          ON size_ordering.name = results_platform.size
        GROUP BY ALL
        ORDER BY rank ASC, throughput_per_dollar DESC
    ;
    """)

con.sql("""
    COPY (
        SELECT
            rank AS "Rank",
            size AS "Size",
            platform AS "Platform",
            environment_name AS "Environment name",
            pricing AS "Pricing (USD)",
            mean_makespan AS "Mean makespan (s)",
            mean_processing_time AS "Mean processing time (s)",
            throughput_per_dollar AS "Throughput per dollar",
            min_runs_per_workload_item AS "Minimum runs per workload item"
        FROM results_platform_price_adjusted) TO 'results_platform_price_adjusted.csv' (SEPARATOR '\t', QUOTE '');
    """)
