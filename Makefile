
CLASSDIR := classes
CLASSPATH := $(CLASSDIR):lib/*:/usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar

JAVA_SOURCE_DIRS=src
JAVA_SOURCES=$(shell find $(JAVA_SOURCE_DIRS) -name '*.java')

#PROJDIRS := clustering core match pipeline textextraction
#SRCDIRS := $(PROJDIRS:%=%/src/main/java)
#TESTDIRS := $(PROJDIRS:%=%/src/test/java)

JAVA=java
JAVAC=javac
JCFLAGS=-d $(CLASSDIR) -Xlint

MKDIR=mkdir

all: compile test

compile: | $(CLASSDIR)
	$(JAVAC) $(JCFLAGS) -cp $(CLASSPATH) $(JAVA_SOURCES)

#test: CLASSPATH := $(CLASSPATH):$(MVNREPO)/org/jmock/jmock/2.5.1/jmock-2.5.1.jar:$(MVNREPO)/org/jmock/jmock-junit4/2.5.1/jmock-junit4-2.5.1.jar:$(MVNREPO)/org/jmock/jmock-legacy/2.5.1/jmock-legacy-2.5.1.jar:$(MVNREPO)/cglib/cglib-nodep/2.1_3/cglib-nodep-2.1_3.jar:$(MVNREPO)/org/objenesis/objenesis/1.0/objenesis-1.0.jar:$(MVNREPO)/org/hamcrest/hamcrest-core/1.1/hamcrest-core-1.1.jar:$(MVNREPO)/org/hamcrest/hamcrest-library/1.1/hamcrest-library-1.1.jar
#test: core-test

#core-test: CLASSPATH := $(CLASSPATH):$(MVNREPO)/org/jmock/jmock/2.5.1/jmock-2.5.1.jar:$(MVNREPO)/org/jmock/jmock-junit4/2.5.1/jmock-junit4-2.5.1.jar:$(MVNREPO)/org/jmock/jmock-legacy/2.5.1/jmock-legacy-2.5.1.jar:$(MVNREPO)/cglib/cglib-nodep/2.1_3/cglib-nodep-2.1_3.jar:$(MVNREPO)/org/objenesis/objenesis/1.0/objenesis-1.0.jar:$(MVNREPO)/org/hamcrest/hamcrest-core/1.1/hamcrest-core-1.1.jar:$(MVNREPO)/org/hamcrest/hamcrest-library/1.1/hamcrest-library-1.1.jar
#core-test:
#	$(JC) $(JCFLAGS) -cp $(CLASSPATH) $(shell find core/src/test/java -name '*.java')
#	cd core ; $(JAVA) -cp ../$(CLASSPATH) org.junit.runner.JUnitCore $(shell grep -l '@Test' `find core/src/test/java -name '*.java'` | sed "s/^\w\+\/src\/test\/java\/\(.*\)\.java$$/\1/" | tr '/' '.') ; cd ..

#jar: pipeline/target/sleuthkit-pipeline-1-SNAPSHOT-job.jar
#
#pipeline/target/sleuthkit-pipeline-1-SNAPSHOT-job.jar: all
#	$(JAVA) -jar /usr/share/java/jarjar.jar 

$(CLASSDIR):
	$(MKDIR) -p $@

clean:
	$(RM) -r $(CLASSDIR)/*

.PHONY: all compile clean test
