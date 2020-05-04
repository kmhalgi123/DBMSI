# DBMSI Project Phase 3

Database for bigtable which stores maps in specific format.

## Installation

Extract the Project_Phase_3 folder and change the libpath in all makefiles to the location on your system. Then run following commands in your folder.

```bash
make db
```
```bash
make test
```

## Usage
Try executing following commands!

```bigt

batchinsert <filename> <type> <tablename> <numbuf>
mapinsert <rowlabel> <columnlabel> <value> <timestamp> <type> <tablename> <numbuf>
query <tablename> <order_type> <rowfilter> <colfilter> <valuefilter> <numbuf>
rowsort <inputbigt> <outputbigt> <colfilter> <order> <numbuf>
rowjoin <leftbigt> <rightbigt> <colfilter> <outputbigt> <numbuf>
getCounts <numbuf>
```

