JDKPATH = /usr/lib/jvm/java-1.8.0-openjdk-amd64
LIBPATH = /home/kaushal/DBMSI/Project_Phase_3
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

hf:bigt.java
	$(JAVAC) bigt.java

hp:BigPage.java
	$(JAVAC) BigPage.java

tuple:Map.java
	$(JAVAC) Map.java

scan:Stream.java
	$(JAVAC) Stream.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
