import subprocess
import os

print("Script started")
print("current directory:")
print(os.getcwd())

p1 = subprocess.run("cd /Users/nik/Desktop/my_projects/dbt/dbt_base_project; pwd; ls -lrt; dbt run", shell=True, capture_output=True, text=True)
print("Printing standard output:")
print(p1.stdout)

print("Printing standard error:")
print(p1.stderr)


print("directory at the end:")
print(os.getcwd())
print("Scrip ends here")


# /Users/nik/PycharmProjects/Airflow_POC/containers/dbt_run/

