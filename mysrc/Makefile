all: wordcount invindex

wordcount:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin WordCount.java 
	jar -cvf ./WordCount.jar -C bin/ .

invindex:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin InvertedIndex.java InvertedIndexPartTwo.java InvertedIndexPartThree.java XmlInputFormat.java
	jar -cvf ./InvertedIndex.jar -C bin/ .
	jar -cvf ./InvertedIndexPartTwo.jar -C bin/ .
	jar -cvf ./InvertedIndexPartThree.jar -C bin/ .

xom:
	javac -cp .:../hadoop-core-1.0.1.jar:../lib/* -d bin XOMtest.java
	java -Xms1024m -Xmx1024m -cp .:../lib/*:bin/ edu.umich.cse.eecs485.XOMtest

clean:
	rm -rf *.jar bin/* output
	rm -rf *.jar bin/* outputTwo
	rm -rf *.jar bin/* outputThree
