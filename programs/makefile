JDKPATH = /usr/lib/jvm/java-11-oracle
LIBPATH = /home/anupriya/DBMSI/DBMSI
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

BatchInsert:BatchInsert.java
	$(JAVAC) BatchInsert.java

batchinsert: BatchInsert
	$(JAVA) programs.BatchInsert