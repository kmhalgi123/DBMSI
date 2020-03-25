JDKPATH = /usr/lib/jvm/java-1.8.0-openjdk-amd64
LIBPATH = /home/kaushal/DBMSI/dbmsi02
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

BatchInsert:BatchInsert.java
	$(JAVAC) BatchInsert.java

batchinsert: BatchInsert
	$(JAVA) programs.BatchInsert