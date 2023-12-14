import json
import glob
import duckdb

con = duckdb.connect("gx-results.duckdb")
con.sql("""CREATE OR REPLACE TABLE timeouts AS SELECT size, timeout_minutes, timeout_minutes * 60 AS timeout_seconds FROM read_csv_auto('input-data/timeouts.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE algorithms AS FROM read_csv_auto('input-data/algorithms.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE datasets AS FROM read_csv_auto('input-data/datasets.csv', header = true);""")
con.sql("""CREATE OR REPLACE TABLE size_ordering AS FROM read_csv_auto('input-data/size_ordering.csv');""")
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
            pricing = 46906.98
        if environment_name == '':
            environment_name = "bare metal, dedicated server"

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
    ;
    """)

# add price-adjusted columns

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT
            *,
            1 / makespan / pricing AS makespan_throughput_per_dollar,
            1 / processing_time / pricing AS processing_throughput_per_dollar,
        FROM results
    ;
    """)

# create list of unique platform names

con.sql("""
    CREATE OR REPLACE TABLE platforms AS
        SELECT DISTINCT
            platform AS platform,
            environment_name AS environment_name
        FROM results
    ;
    """)

# create the required workload for each size category

con.sql("""
    CREATE OR REPLACE TABLE workload AS
        SELECT
            datasets.name AS dataset,
            datasets.size AS size,
            algorithms.name AS algorithm,
            platforms.platform AS platform,
            platforms.environment_name AS environment_name
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
            coalesce(makespan_throughput_per_dollar, 0.0) AS makespan_throughput_per_dollar,
            coalesce(processing_throughput_per_dollar, 0.0) AS processing_throughput_per_dollar,
            coalesce(runs, 3) AS runs
        FROM workload
        LEFT JOIN results
               ON workload.dataset = results.dataset
              AND workload.algorithm = results.algorithm
              AND workload.platform = results.platform
              AND workload.environment_name = results.environment_name
        JOIN timeouts
          ON timeouts.size = workload.size
        JOIN platforms
          ON platforms.platform = workload.platform
         AND platforms.environment_name = workload.environment_name
    ;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_platform_algorithm AS
        SELECT
            size,
            platform,
            max(environment_name) AS environment_name,
            max(pricing) AS pricing,
            algorithm,
            avg(makespan) AS mean_makespan,
            avg(processing_time) AS mean_processing_time,
            1/avg(1/makespan_throughput_per_dollar) AS makespan_throughput_per_dollar,
            1/avg(1/processing_throughput_per_dollar) AS processing_throughput_per_dollar,
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
            avg(pricing) AS pricing, -- NULL prices should be consumed by max()
            avg(mean_makespan) AS mean_makespan,
            avg(mean_processing_time) AS mean_processing_time,
            1/avg(1/makespan_throughput_per_dollar) AS makespan_throughput_per_dollar,
            1/avg(1/processing_throughput_per_dollar) AS processing_throughput_per_dollar,
            min(min_runs_per_workload_item) AS min_runs_per_workload_item
        FROM results_platform_algorithm
        GROUP BY ALL
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
            makespan_throughput_per_dollar,
            processing_throughput_per_dollar,
            min(min_runs_per_workload_item) AS min_runs_per_workload_item
        FROM results_platform
        JOIN size_ordering
          ON size_ordering.name = results_platform.size
        GROUP BY ALL
        ORDER BY rank ASC, processing_throughput_per_dollar DESC
    ;
    """)

con.sql("""
    COPY (
        SELECT
            row_number() OVER (PARTITION BY rank ORDER BY rank) AS "Position",
            size AS "Size",
            platform AS "Platform",
            environment_name AS "Environment name",
            pricing AS "Pricing (USD)",
            mean_makespan AS "Mean makespan (s)",
            mean_processing_time AS "Mean processing time (s)",
            1000000*makespan_throughput_per_dollar AS "Makespan throughput per dollar",
            1000000*processing_throughput_per_dollar AS "Processing throughput per dollar",
            min_runs_per_workload_item AS "Minimum runs per workload item"
        FROM results_platform_price_adjusted
        ORDER BY rank)
        TO 'results_platform_price_adjusted.csv' (SEPARATOR '\t', QUOTE '');
    """)
