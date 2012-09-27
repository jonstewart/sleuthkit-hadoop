
CLASSDIR=bin

JAVA_SOURCE_DIR=src
JAVA_SOURCES=$(shell find $(JAVA_SOURCE_DIR) -name '*.java')
JAVA_CLASSES=$(patsubst $(JAVA_SOURCE_DIR)/%.java,$(CLASSDIR)/%.class,$(JAVA_SOURCES))
JAVA_CLASSPATH=$(CLASSDIR)/src:lib/*:/usr/lib/hadoop-0.20/hadoop-core.jar:/usr/lib/hbase/hbase-0.90.6-cdh3u4.jar

TEST_SOURCE_DIR=test
TEST_SOURCES=$(shell find $(TEST_SOURCE_DIR) -name '*.java')
TEST_CLASSES=$(patsubst $(TEST_SOURCE_DIR)/%.java,$(CLASSDIR)/%.class,$(TEST_SOURCES))
TEST_CLASSPATH=$(JAVA_CLASSPATH):$(CLASSDIR)/test:lib-test/*:/usr/lib/hadoop-0.20/lib/guava-r09-jarjar.jar

#PROJDIRS := clustering core match pipeline textextraction
#SRCDIRS := $(PROJDIRS:%=%/src/main/java)
#TESTDIRS := $(PROJDIRS:%=%/src/test/java)

JAVA=java
JAVAC=javac
JCFLAGS=-Xlint

MKDIR=mkdir

all: compile test

compile: $(JAVA_SOURCES) | $(CLASSDIR)/src
	$(JAVAC) -d $(CLASSDIR)/src $(JCFLAGS) -cp $(JAVA_CLASSPATH) $(JAVA_SOURCES)

test: $(TEST_SOURCES) | $(CLASSDIR)/test
	$(JAVAC) -d $(CLASSDIR)/test $(JCFLAGS) -cp $(TEST_CLASSPATH) $(TEST_SOURCES)
	$(JAVA) -cp $(TEST_CLASSPATH) org.junit.runner.JUnitCore com.lightboxtechnologies.test.UnitTests

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

$(CLASSDIR) $(CLASSDIR)/src $(CLASSDIR)/test:
	$(MKDIR) -p $@

clean:
	$(RM) -r $(CLASSDIR)/*

.PHONY: all clean compile test
