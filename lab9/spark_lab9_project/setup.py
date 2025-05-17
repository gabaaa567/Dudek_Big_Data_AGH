from setuptools import setup, find_packages

setup(
    name="spark_lab9_project",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "pyspark"
    ],
    entry_points={
        "console_scripts": [
            "run-spark-job=spark_lab9_project.spark_job:main"
        ]
    }
)
