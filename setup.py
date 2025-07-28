from setuptools import setup, find_packages

setup(
    name="hexa",
    version="0.1.0",
    description="Abbott AI Analysis MVP - Natural language to SQL insights pipeline",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "python-dotenv",
        "typer[all]",
        "rich", 
        "duckdb",
        "pandas",
        "pyyaml",
        "openpyxl",
        "xlrd",
        "pyxlsb",
    ],
    entry_points={
        "console_scripts": [
            "hexa=hexa.cli:app",
        ],
    },
    python_requires=">=3.8",
)