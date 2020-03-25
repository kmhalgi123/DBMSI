#This Makefile is used to create assignments.

#the following macros should be updated according to
#the assignment to be generated

JARFILES=bufmgr/*.class diskmgr/*.class global/*.class iterator/*.class\
         heap/*.class chainexception/*.class  btree/*.class index/*.class tests/*.class

JDKPATH = /usr/lib/jvm/java-1.8.0-openjdk-amd64
LIBPATH = .:..
CLASSPATH = $(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/bin/java  -classpath $(CLASSPATH)

DOCFILES=bufmgr diskmgr global chainexception heap btree iterator index

##############  update the above for each assignment in making

ASSIGN=/home/kaushal/DBMSI/dbmsi02
LIBDIR=$(ASSIGN)/lib
KEY=$(ASSIGN)/key
SRC=$(ASSIGN)/src

PACKAGEINDEX=$(ASSIGN)/javadoc
IMAGELINK=$(PACKAGEINDEX)/images

JAVADOC=javadoc -public -d $(PACKAGEINDEX)

### Generate jar and javadoc files.  Apply to most assignments.
db: 
	make -C global
	make -C chainexception
	##make -C btree
	make -C bufmgr
	make -C diskmgr
	make -C BigT
	make -C index
	make -C iterator
	
doc:
	$(JAVADOC) $(DOCFILES)

test: 
	cd programs; make batchinsert

clean:
	\rm -f $(CLASSPATH)/*.class *~ \#* core $(JARFILES) TRACE
