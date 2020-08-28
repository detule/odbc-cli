# odbc-cli

*Please note: this package should be considered "alpha" - while you are more than welcome to use it, you should expect that getting it to work for you will require quite a bit of self-help on your part.  At the same time, it may be a great opportunity for those that want to contribute.*

[**odbc-cli**](https://github.com/detule/odbcli) is an interactive command line query tool intended to work for DataBase Management Systems (DBMS) supported by ODBC drivers.

As is the case with the [remaining clients](https://github.com/dbcli/) derived from the [python prompt toolkit library](https://github.com/prompt-toolkit/python-prompt-toolkit), **odbc-cli** also supports a rich interactive command line experience, with features such as auto-completion, syntax-highlighting, multi-line queries, and query-history.

Beyond these, some distinguishing features of **odbc-cli** are:

- **Multi DBMS support**:  In addition to supporting connections to multiple DBMS, with **odbc-cli** you can connect to, and query multiple databases in the same session.
- **An integrated object browser**: Navigate between connections and objects within a database.
- **Small footprint and excellent performance**: One of the main motivations is to reduce both the on-disk, as well as in-memory footprint of the [existing Microsoft SQL Server client](https://github.com/dbcli/mssql-cli/), while at the same time improve query execution, and time spent retrieving results.

![odbc-cli objectbrowser](https://github.com/detule/odbcli-screenshots/raw/master/odbcli-basic.gif)

## Installing and OS support

The assumption is that the starting point is a box with a working ODBC setup.  This means a driver manager (UnixODBC, for example), together with ODBC drivers that are appropriate to the DBM Systems you intend to connect to.

To install this package, simply:

```sh
python -m pip install odbcli
```

Notes:
* In theory, this package should work under Windows, MacOS, as well as Linux.  I can only test Linux; help testing and developing on the other platforms (as well as Linux) is very much welcome.
* The main supporting package, [**cyanodbc**](https://github.com/cyanodbc/cyanodbc) comes as a pre-compiled wheel.  It requires a modern C++ library supporting the C++14 standard.  The cyanodbc linux wheel is built on Ubuntu 16 - not exactly bleeding edge.  Anything newer should be fine.

## Usage

### Starting / Exiting the client

After installing the python module, you can fire up the client by executing

```sh
odbc-cli
```

or, alternatively:

```sh
python -m odbcli.__main__
```

You may exit the client at any point by entering `[CTRL+q]`

### Connecting to databases

**odbc-cli** discovers and populates a list of available connections by querying your ODBC driver manager.  Therefore, in order to see your connections in the client, you must have them listed in the INI configuration files appropriate to your driver manager.  For example, with unixODBC, consider starting the client by specifying the following environment variables:

```sh
ODBCSYSINI=/path/to/driver/configuration/file ODBCINSTINI=name-of-driver-configuration-file ODBCINI=/path/and/name/of/user/dbms/configuration/file odbc-cli
```

The client starts with the object browser visible to the right, listing all the connections discovered by the driver manager.

### The object browser

This panel can be shown/hidden by using the `[CTRL+t]` key combination.  Initially, only the available connections are listed - these play the role of "root" nodes in a hierarchical tree of database objects.  You can navigate between these objects using the arrow keys:  in addition to the up and down keys, pressing the right arrow is equivalent to asking the object to "expand" and itemize enclosing objects, be it catalogs, schemas, or tables.   Pressing the left arrow whilst in the object browser is equivalent to asking the object to "collapse" and hide its "children".  Therfore, when a connection is selected, pressing the right arrow will bring up a username/password connection dialog.  After succesfully connecting to the database, you can expand/collapse the connection, catalogs, schemas, and tables - expanding a table brings up information about its column structure.

This buffer is searchable - pressing the search key appropriate to your editing mode (vim or emacs) brings up a search bar at the top of the buffer that you can then use to zero-in on the particular table, for example, that you may be looking for.

Pressing the left arrow when a connection that is fully collapsed is highlighted brings up a disconnect dialog.  Pressing the return key on connections that are connected allows you to toggle each of them as "active" - meaning, the one used to execute queries in the main window against.

### The preview window

Pressing the return key when a table or a view is highlighted in the object browser, brings up the preview window.  There are three elements in this window: the input/filtering buffer, the `Done` button, and the main output buffer.  Pressing the `tab` button will toggle the focus between these three elements.

You can preview table contents - the equivalent of `SELECT *` - by simply pressing `return` while the cursor is in the filtering input buffer at the top of the preview window (equivalent to no filtering, and an unadulterated SELECT).  Repeatedly pressing the return key will page through the results.  In addition you can filter (or order) the output by entering a `WHERE ...` or similar qualifiers in the input buffer.  Tabing over to the `Done` button and pressing `return` will close the preview window.

![odbc-cli tablepreview](https://github.com/detule/odbcli-screenshots/raw/master/odbcli-preview.gif)

### The main query execution buffer

Closing the object browser (`[CTRL+t]`) leaves you focused on the main buffer allowing you to execute statements against the currently active connection. After executing a query the results are piped to the system pager, or `less` if it is available.  If it is not, and there is no system pager configured, consider pip-installing `pypager`.

This buffer sports both, an as-you-type auto-completion, as well as suggestions drawn from the history file.

### Cancelling queries

Outside of queries to support auto completion, all statements are executed in a separate process, "executor".  Whether in the main execution buffer, or in the table preview, you should be able to cancel a long-running query by pressing `[CTRL+c]`.  At that point, the client will attempt to close and re-start the "executor" process.

### Configuring the client

A limited set of configuration options are made available in the `config` file in the directory:

* If the `XDG_CONFIG_HOME` environment variable is defined then, within its `odbcli` sub-folder.
* else, if the platform is `Windows`, then in `os.getenv("USERPROFILE") + "\\AppData\\Local\\dbcli\\odbcli\\"`
* Otherwise in `~/.config/odbcli/`

TODO: Document the options here in the readme.

## Supported DBMS

* Microsoft SQL Server

* MySQL

* SQLite

* PostgreSQL

TODO: Document in greater detail which features are working for which DBMS.

Further details forthcoming ...
