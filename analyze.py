import json
import glob
import duckdb

con = duckdb.connect("gx-results.duckdb")
con.sql("""CREATE OR REPLACE TABLE datasets   AS FROM read_csv_auto('datasets.csv',   header = true);""")
con.sql("""CREATE OR REPLACE TABLE algorithms AS FROM read_csv_auto('algorithms.csv', header = true);""")
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

                con.sql(f"INSERT INTO results VALUES ('{platform}', '{job_id}', '{algorithm}', '{dataset}', '{run_id}', {success}, {load_time}, {makespan}, {processing_time});")

con.sql("""COPY results TO 'results.csv';""")

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT * EXCLUDE name
        FROM results
        JOIN datasets ON datasets.name = results.dataset;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT *
        FROM results
        WHERE success = true
          AND size IN ('S', 'M', 'L', 'XL', '2XL', '3XL');
    """)

con.sql("""
    CREATE OR REPLACE TABLE results AS
        SELECT platform, algorithm, dataset, size, mean(processing_time) AS time
        FROM results
        GROUP BY platform, algorithm, dataset, size;
    """)

con.sql("""
    CREATE OR REPLACE TABLE results_wide AS
        PIVOT results
        ON algorithm
        USING sum(time);
    """)
