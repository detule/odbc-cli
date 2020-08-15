import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()


install_requirements = [
    'cyanodbc >= 0.0.2',
    'prompt_toolkit >= 3.0.5',
    'Pygments>=2.6.1',
    'sqlparse >= 0.3.1',
    'configobj >= 5.0.6',
    'click >= 7.1.2',
    'cli_helpers >= 2.0.1'
]

setuptools.setup(
    name = "odbcli", # Replace with your own username
    version = "0.0.1",
    author = "Oliver Gjoneski",
    author_email = "ogjoneski@gmail.com",
    description = "ODBC Client",
    license = 'MIT',
    long_description = long_description,
    long_description_content_type = "text/markdown",
    install_requires = install_requirements,
    url = "https://github.com/pypa/odbc-cli",
    scripts=[
        'odbc-cli'
        ],
    packages = setuptools.find_packages(),
    include_package_data = True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires = '>=3.5',
)
