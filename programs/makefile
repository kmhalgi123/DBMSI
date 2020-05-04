JDKPATH = /usr/lib/jvm/java-1.8.0-openjdk-amd64
LIBPATH = /home/kaushal/DBMSI/Project_Phase_3
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

MapInsert:MapInsert.java
	$(JAVAC) MapInsert.java

getCounts:getCounts.java
	$(JAVAC) getCounts.java

RowJoin:RowJoin.java
	$(JAVAC) RowJoin.java

RowSort:RowSort.java
	$(JAVAC) RowSort.java

BatchInsert:BatchInsert.java
	$(JAVAC) BatchInsert.java

DBMSI:DBMSI.java
	$(JAVAC) DBMSI.java

Query:Query.java
	$(JAVAC) Query.java

TestDriver:TestDriver.java
	$(JAVAC) TestDriver.java

together:*.java
	$(JAVAC) *.java

dbmsi: DBMSI
	$(JAVA) programs.DBMSI
