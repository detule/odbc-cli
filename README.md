# odbc-cli

[**odbc-cli**](https://github.com/detule/odbcli) is an interactive command line query tool intended to work for DataBase Management Systems (DBMS) supported by ODBC drivers.

As is the case with the [remaining clients](https://github.com/dbcli/) derived from the [python prompt toolkit library](https://github.com/prompt-toolkit/python-prompt-toolkit), **odbc-cli** also supports a rich interactive command line experience, with features such as auto-completion, syntax-highlighting, multi-line queries, and query-history.

Beyond these, some distinguishing features of **odbc-cli** are:

- **Multi DBMS support**:  In addition to supporting connections to multiple DBMS, with **odbc-cli** you can connect to, and query multiple databases in the same session.
- **An integrated object browser**: Navigate between connections and objects within a database.
- **Small footprint and excellent performance**: One of the main motivations is to reduce both the on-disk as well as in-memory footprint of the [existing Microsoft SQL Server client](https://github.com/dbcli/mssql-cli/), while at the same time improve query execution, and time spent retrieving results.

![odbc-cli objectbrowser](https://github.com/detule/odbcli-screenshots/raw/master/odbcli-basic.gif)

## Installing and OS support

The assumption is that the starting point is a box with a working ODBC setup.  This means a driver manager (UnixODBC, for example), together with ODBC drivers that are appropriate to the DBM Systems you intend to connect to.

To install this package, simply:

```sh
python -m pip install odbcli
```

Notes:
* In theory package should work under Windows, MacOS, as well as Linux.  I can only test Linux; help testing and developing on the other platforms (as well as Linux) is very welcome.
* The main supporting package, [**cyanodbc**](https://github.com/cyanodbc/cyanodbc) comes as a pre-compiled wheel.  It requires a modern C++ library supporting the C++14 standard.  The cyanodbc linux wheel is built on Ubuntu 16 - not exactly bleeding edge.  Anything newer should be fine.

## Usage

## Supported DBMS

* Microsoft SQL Server

* MySQL

* SQLite

* PostgreSQL

![odbc-cli tablepreview](https://github.com/detule/odbcli-screenshots/raw/master/odbcli-preview.gif)

Further details forthcoming ...
