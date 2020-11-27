bin/hadoop com.sun.tools.javac.Main ../TriangleCounting/*.java
jar cf ../TriangleCounting/triangle-counting.jar ../TriangleCounting/*.class
bin/hadoop jar ../TriangleCounting/triangle-counting.jar TriangleCount ../TriangleCounting/data/sample ../TriangleCounting/data/output
