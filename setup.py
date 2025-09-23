from setuptools import setup, find_packages

setup(
    name="top_items_detection",              # Project name for the test
    version="0.1.0",
    packages=find_packages(where="src"),  # Include all packages under src/
    package_dir={"": "src"},              # Source code folder
    install_requires=[
        "pyspark>=3.0.0",                # PySpark for RDD and DataFrame
        "pytest",                        # For testing
        "flake8"                         # For linting/style checks
    ],
    entry_points={
        "console_scripts": [
            "run_top_items=main:main",   # Optional CLI command to run main.py
        ],
    },
    python_requires=">=3.8",              # Minimum Python version
    author="",
    description="Compute top X detected items per loc",
    url="https://github.com/rainfrost99/test",  # GitHub repo
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)