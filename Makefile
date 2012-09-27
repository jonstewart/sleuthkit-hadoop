CLASSDIR=bin

JAVA_SOURCE_DIR=src
JAVA_SOURCES=$(shell find $(JAVA_SOURCE_DIR) -name '*.java')
JAVA_CLASSES=$(patsubst $(JAVA_SOURCE_DIR)/%.java,$(CLASSDIR)/$(JAVA_SOURCE_DIR)/%.class,$(JAVA_SOURCES))
JAVA_CLASSPATH=$(CLASSDIR)/src:lib/*:/usr/lib/hadoop/hadoop-common.jar:/usr/lib/hadoop/hadoop-annotations.jar:/usr/lib/hadoop-0.20-mapreduce/hadoop-core.jar:/usr/lib/hbase/hbase.jar

TEST_SOURCE_DIR=test
TEST_SOURCES=$(shell find $(TEST_SOURCE_DIR) -name '*.java')
TEST_CLASSES=$(patsubst $(TEST_SOURCE_DIR)/%.java,$(CLASSDIR)/$(TEST_SOURCE_DIR)/%.class,$(TEST_SOURCES))
TEST_CLASSPATH=$(JAVA_CLASSPATH):$(CLASSDIR)/test:lib-test/*:/usr/lib/hadoop/lib/guava-11.0.2.jar

JAVA=java
JAVAC=javac
JCFLAGS=-Xlint
JAR=jar

MKDIR=mkdir

all: compile test jar

compile: $(JAVA_SOURCES) | $(CLASSDIR)/src
	$(JAVAC) -d $(CLASSDIR)/src $(JCFLAGS) -cp $(JAVA_CLASSPATH) $(JAVA_SOURCES)

test: compile $(TEST_SOURCES) | $(CLASSDIR)/test
	$(JAVAC) -d $(CLASSDIR)/test $(JCFLAGS) -cp $(TEST_CLASSPATH) $(TEST_SOURCES)
	$(JAVA) -cp $(TEST_CLASSPATH) org.junit.runner.JUnitCore com.lightboxtechnologies.test.UnitTests

jar: compile test
	$(JAR) cf $(CLASSDIR)/tp.jar -C $(CLASSDIR)/src .

$(CLASSDIR) $(CLASSDIR)/src $(CLASSDIR)/test:
	$(MKDIR) -p $@

clean:
	$(RM) -r $(CLASSDIR)/*

.PHONY: all clean compile jar test
