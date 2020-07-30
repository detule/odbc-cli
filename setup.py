import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name = "odbcli", # Replace with your own username
    version = "0.0.1",
    author = "Oliver Gjoneski",
    author_email = "ogjoneski@gmail.com",
    description = "ODBC Client",
    license = 'MIT',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    url = "https://github.com/pypa/sampleproject",
    scripts=[
        'odbc-cli'
        ],
    packages = setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires = '>=3.5',
)
