@echo off
set PYTHONHOME=c:\python37

if exist dist rd /s /q dist

python -m pip install --upgrade setuptools wheel
python setup.py bdist_wheel
if exist build rd /s /q build
if exist gcbmwalltowall.egg-info rd /s /q gcbmwalltowall.egg-info
