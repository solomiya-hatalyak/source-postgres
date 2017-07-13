from distutils.core import setup

setup(
    name="panoply_postgres",
    version="1.0.0",
    description="Panoply Data Source for Postgres",
    author="Ofir Herzas",
    author_email="ofirh@panoply.io",
    url="http://panoply.io",
    install_requires=[
        "panoply-python-sdk==1.3.4",
        "mock==2.0.0",
        "psycopg2==2.7.1"
    ],
    extras_require={
        "test": [
            "pep8==1.7.0",
            "coverage==4.3.4"
        ]
    },

    # place this package within the panoply package namespace
    package_dir={"panoply": ""},
    packages=["panoply.postgres"]
)
