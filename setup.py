# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from glob import glob

setup(
    name="gcbmwalltowall",
    version="2.2.4",
    description="gcbmwalltowall",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=[
        "mojadata", "sqlalchemy", "pandas", "openpyxl", "spatial_inventory_rollback",
        'sqlalchemy-access; platform_system == "Windows"'
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