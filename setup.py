# Always prefer setuptools over distutils
from setuptools import setup, find_packages
from glob import glob

setup(
    name="gcbmwalltowall",
    version="0.1",
    description="gcbmwalltowall",
    classifiers=[
        "Programming Language :: Python :: 3",
    ],
    packages=find_packages(exclude=["contrib", "docs", "tests"]),
    install_requires=[
        "mojadata",
    ],
    extras_require={},
    package_data={},
    data_files=[
        ("Tools/gcbmwalltowall", ["files/settings.json"]),
    ],
    entry_points={
        "console_scripts": [
            "walltowall = gcbmwalltowall.application.walltowall:cli",
        ]
    },
    python_requires=">=3.7"
)