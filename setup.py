#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-google-analytics",
    version="0.0.1",
    description="Singer.io tap for extracting data from the Google Analytics Reporting API",
    author='Said Tezel',
    author_email="said@pamukk.com",
    url="https://github.com/saidtezel/tap-google-analytics",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_google_analytics"],
    install_requires=[
        "singer-python==5.9.0",
        "google-api-python-client==1.7.11",
        "oauth2client==4.1.3",
        "backoff==1.8.0"
    ],
    entry_points="""
    [console_scripts]
    tap-google-analytics=tap_google_analytics:main
    """,
    packages=["tap_google_analytics"],
    package_data = {
      'tap_google_analytics/defaults': [
        "default_report_definition.json",
      ],
    },
    include_package_data=True,
)
