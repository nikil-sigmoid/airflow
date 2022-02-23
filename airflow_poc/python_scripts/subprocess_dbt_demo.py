import subprocess

p1 = subprocess.run("cd /Users/nik/PycharmProjects/Airflow_POC/containers/dbt_run/dbt_sample; dbt run ", shell=True, capture_output=True, text=True)
print("Printing standard output:")
print(p1.stdout)

print("Printing standard error:")
print(p1.stderr)



# p2 = subprocess.run("cd /Users/nik/dbt_sample; pwd; dbt run", capture_output=True, text=True)
# print(p2.stdout)
# subprocess.run("dbt run", shell=True)
