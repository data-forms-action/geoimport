# geoimport
####Max Mind GEO IP Data Importer for Postgres

Importer program for GEO IP City data and the Geoname location data that it references.

This program will import these .csv files into a _**Postgres**_ database.

Works for Mac OS X or Linux. Compile with clang.

**Linux dependencies** Requires: libdispatch libbsd
apt-get install libbsd-dev libdispatch-dev


Running the Program:
```
./geoimport
Max Mind GEO IP Data importer.
Populates Geo ip tables on Postgres from a .csv file.

Options:
	-D Specify database name. Host will be localhost and user will be login of user executing the program.
	Use this to connect to your local database server as yourself.

	-P[1-999] Specify number of processing threads. Default will be up to 3 if not specified.

	-U Specify Postgres connection string OR URL (cannot be used in conjunction with -D).
	Connection string: 'host=localhost port=5432 dbname=mydb connect_timeout=10'
	  * For details see: https://www.postgresql.org/docs/current/static/libpq-connect.html#LIBPQ-PARAMKEYWORDS
	OR
	Connection URI: 'postgresql://[user[:password]@][netloc][:port][/dbname][?param1=value1&...]' 

Usage:	geoimport -P4 -D [dbname] /file/to/import.csv
Usage:	geoimport -P4 -U 'host=localhost port=5432 dbname=mydb connect_timeout=10' /file/to/import.csv
Usage:	geoimport -P4 -U 'postgresql://user@localhost/mydb?connect_timeout=10&application_name=myapp' /file/to/import.csv
```

###Building
Build on Linux with the following (Ubuntu)

```
clang++ main.cpp -o geoimport -fblocks -std=c++11 -D_BSD_SOURCE -I/usr/include/postgresql -L/usr/lib -lBlocksRuntime -lpthread -lpq -ldispatch -lbsd  
```

