JDKPATH = /Library/Java/JavaVirtualMachines/jdk1.8.0_251.jdk/
LIBPATH = /Users/christianbagaya/Documents/ASU/Spring_sem_1/DBMSI/Phase_1/minjava/javaminibase/src
CLASSPATH = $(JDKPATH)/lib/classes.zip:.:..:$(LIBPATH)
BINPATH = $(JDKPATH)/bin
JAVAC = $(JDKPATH)/Contents/Home/bin/javac -classpath $(CLASSPATH)
JAVA  = $(JDKPATH)/Contents/Home/bin/javac  -classpath $(CLASSPATH)

PROGS = together

all: $(PROGS)

bm:BufMgr.java
	$(JAVAC) BufMgr.java

together:*.java
	$(JAVAC) *.java

clean:
	\rm -f *.class *~ \#* core 
