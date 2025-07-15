# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from glob import glob

setup(
    name="gcbmwalltowall",
    version="2.1.5",
    description="gcbmwalltowall",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=[
        "mojadata>=4.0.3", "sqlalchemy", "sqlalchemy-access", "pandas", "openpyxl", "spatial_inventory_rollback>=1.1.0"
    ],
    extras_require={},
    package_data={},
    data_files=[
        ("Tools/gcbmwalltowall",                   ["files/settings.json"]),
        ("Tools/gcbmwalltowall/templates/default", glob("files/templates/default/*"))
    ],
    entry_points={
        "console_scripts": [
            "walltowall = gcbmwalltowall.application.walltowall:cli",
        ]
    },
    python_requires=">=3.7"
)