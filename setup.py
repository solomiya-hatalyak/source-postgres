from distutils.core import setup

setup(
    name="panoply_postgres",
    version="3.0.0",
    description="Panoply Data Source for Postgres",
    author="Panoply Dev Team",
    author_email="support@panoply.io",
    url="http://panoply.io",
    install_requires=[
        "panoply-python-sdk@git+ssh://git@github.com/panoplyio/"
        "panoply-python-sdk.git@v2.0.0#egg=panoply-python-sdk",
        "psycopg2==2.8.5",
        "backoff==1.4.3"
    ],
    extras_require={
        "test": [
            "pycodestyle==2.5.0",
            "coverage==4.3.4",
            "mock==2.0.0"
        ]
    },

    # place this package within the panoply package namespace
    package_dir={"panoply": ""},
    packages=["panoply.postgres"]
)
