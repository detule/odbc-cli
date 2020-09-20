# odbc-cli

*Please note: this package should be considered "alpha" - while you are more than welcome to use it, you should expect that getting it to work for you will require quite a bit of self-help on your part.  At the same time, it may be a great opportunity for those that want to contribute.*

<p align="center">
<img src="https://github.com/detule/odbcli-screenshots/raw/master/object-browser-still.png" width="45%"> &nbsp;&nbsp;&nbsp; <img src="https://github.com/detule/odbcli-screenshots/raw/master/preview-still.png" width="45%">
</p>
<p align="center">
<img src="https://github.com/detule/odbcli-screenshots/raw/master/query-buffer-still.png" width="70%">
</p>

[**odbc-cli**](https://github.com/detule/odbc-cli) is an interactive command line query tool intended to work for DataBase Management Systems (DBMS) supported by ODBC drivers.

As is the case with the [remaining clients](https://github.com/dbcli/) derived from the [python prompt toolkit library](https://github.com/prompt-toolkit/python-prompt-toolkit), **odbc-cli** also supports a rich interactive command line experience, with features such as auto-completion, syntax-highlighting, multi-line queries, and query-history.

Beyond these, some distinguishing features of **odbc-cli** are:

- **Multi DBMS support**:  In addition to supporting connections to multiple DBMS, with **odbc-cli** you can connect to, and query multiple databases in the same session.
- **An integrated object browser**: Navigate between connections and objects within a database.
- **Small footprint and excellent performance**: One of the main motivations is to reduce both the on-disk, as well as in-memory footprint of the [existing Microsoft SQL Server client](https://github.com/dbcli/mssql-cli/), while at the same time improve query execution, and time spent retrieving results.
- **Out-of-database auto-completion**: Mostly relevant to SQL Server users, but auto-completion is "aware" of schema and table structure outside of the currently connected catalog / database.

## Installing and OS support

The assumption is that the starting point is a box with a working ODBC setup.  This means a driver manager (UnixODBC, for example), together with ODBC drivers that are appropriate to the DBM Systems you intend to connect to.

To install the latest version of the package marked as *stable*, simply:

```sh
python -m pip install odbcli
```

*Development* versions, tracking the tip of the master branch, are hosted on Test Pypi, and can be installed, for example by:

```sh
python -m pip install --index-url https://test.pypi.org/simple/ odbcli
```

Notes:
* In theory, this package should work under Windows, MacOS, as well as Linux.  I can only test Linux; help testing and developing on the other platforms (as well as Linux) is very much welcome.
* The main supporting package, [**cyanodbc**](https://github.com/cyanodbc/cyanodbc) comes as a pre-compiled wheel.  It requires a modern C++ library supporting the C++14 standard.  The cyanodbc Linux wheel is built on Ubuntu 16 - not exactly bleeding edge.  Anything newer should be fine.
  * As of https://github.com/detule/odbc-cli/commit/bea22885d0483de0c1899ebc26ff853568b0e417, **odbc-cli** requires `cyanodbc` version [0.0.2.136](https://test.pypi.org/project/Cyanodbc/0.0.2.136/#files) or newer.

## Usage

See the [Usage section here](https://detule.github.io/odbc-cli/index.html#Usage).

## Supported DBMS

I have had a chance to test connectivity and basic functionality to the following DBM Systems:

* **Microsoft SQL Server**
  Support and usability here should be furthest along.  While I encounter (and fix) an occasional issue, I use this client in this capacity daily.

  Driver notes:
  * OEM Driver: No known issues (I test with driver version 17.5).
  * FreeTDS: Please use version 1.2 or newer for optimal performance (older versions do not support the SQLColumns API endpoint applied to tables out-of-currently-connected-catalog).

* **MySQL**
  I have had a chance to test connectivity and basic functionality, but contributor help very much appreciated.

* **SQLite**
  I have had a chance to test connectivity and basic functionality, but contributor help very much appreciated.

* **PostgreSQL**
  I have had a chance to test connectivity and basic functionality, but contributor help very much appreciated.

  Driver notes:
  * Please consider using [psqlODBC 12.01](https://odbc.postgresql.org/docs/release.html) or newer for optimal performance (older versions, when used with a PostgreSQL 12.0, seem to have a documented bug when calling into SQLColumns).

* **Snowflake**
  I have had a chance to test connectivity and basic functionality, but contributor help very much appreciated.

  Driver notes:
  * As of version 2.20 of their ODBC driver, consider specifying the `Database` field in the DSN configuration section in your INI files.  If no `Database` is specified when connecting, their driver will report the empty string - despite being attached to a particlar catalog.  Subsequently, post-connection specifying the database using `USE` works as expected.

* **Other** DMB Systems with ODBC drivers not mentioned above should work with minimal, or hopefully no additional, configuration / effort.

## Reporting issues

The best feature - multi DBMS support, is also a curse from a support perspective, as there are too-many-to-count combinations of:

* Client platform (ex: Debian 10)
* Data base system (ex: SQL Server)
* Data base version (ex: 19)
* ODBC driver manager (ex: unixODBC)
* ODBC driver manager version (ex: 2.3.x)
* ODBC driver (ex: FreeTDS)
* ODBC driver version (ex: 1.2.3)

that could be specific to your setup, contributing to the problem and making it difficult to replicate.  Please consider including all of this information when reporting the issue, but above all be prepared that I may not be able to replicate and fix your issue (and therefore, hopefully you can contribute / code-up a solution).  Since the use case for this client is so broad, the only way I see this project having decent support is if we build up a critical mass of user/developers.

## Troubleshooting

### Listing connections and connecting to databases

The best way to resolve connectivity issues is to work directly in a python console.  In particular, try working directly with the `cyanodbc` package in an interactive session.

* When starting the client, **odbc-cli** queries the driver manager for a list of available connections by executing:

```
import cyanodbc
cyanodbc.datasources()
```

Make sure this command returns a coherent output / agrees with your expectations before attempting anything else.  If it does not, consult the documentaion for your driver manager and make sure all the appropriate INI files are populated accordingly.

* If for example, you are attempting to connect to a DSN called `postgresql_db` - recall this should be defined and configured in the INI configuration file appropriate to your driver manager, in the background, **odbc-cli** attempts to establish a connection with a connection string similar to:

```
import cyanodbc
conn = cyanodbc.connect("DSN=postgresql_db;UID=postgres;PWD=password")
```

If experiencing issues connecting to a database, make sure you can establish a connection using the method above, before moving on to troubleshoot other parts of the client.

## Acknowledgements

This project would not be possible without the most excellent [python prompt toolkit library](https://github.com/prompt-toolkit/python-prompt-toolkit).  In addition, idea and code sharing between the [clients that leverage this library](https://github.com/dbcli/) is rampant, and this project is no exception - a big thanks to all the `dbcli` contributors.
