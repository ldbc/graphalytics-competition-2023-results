import json
import glob
import duckdb

with open("results.csv", "w") as of:
    of.write("platform,job,algorithm,dataset,run,success,load_time,makespan,processing_time\n")

    for path in glob.glob("*.json"):
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

                    of.write(f"{platform},{job_id},{algorithm},{dataset},{run_id},{success},{load_time},{makespan},{processing_time}\n")

# TODO: Pivot
