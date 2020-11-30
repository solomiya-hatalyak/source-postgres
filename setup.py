import os
from distutils.core import setup

token = os.environ.get("CI_USER_GITHUB_TOKEN", "")

required_packages = [
    'panoply-python-sdk @ git+https://{}@github.com/panoplyio/'
    'panoply-python-sdk.git@v2.0.2#egg==panoply-python-sdk'.format(token),
    "psycopg2==2.8.5",
    ]

setup(
    name="panoply_postgres",
    version="3.0.3",
    description="Panoply Data Source for Postgres",
    author="Panoply Dev Team",
    author_email="support@panoply.io",
    url="http://panoply.io",
    install_requires=required_packages,
    extras_require={
        "test": [
            "pycodestyle==2.5.0",
            "coverage==4.3.4",
            "mock==2.0.0"
        ]
    },

    # place this package within the panoply package namespace
    package_dir={"panoply": ""},
    packages=["panoply.postgresv2", "panoply.postgresv2.dal",
              "panoply.postgresv2.dal.queries"]
)
