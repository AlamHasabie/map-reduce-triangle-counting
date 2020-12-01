# map-reduce-triangle-counting
Closed triplets counting with map-reduce

## How to run
1. Compile : bin/hadoop com.sun.tools.javac.Main \[PATH_TO_DIR\]/*.java

For each phase, run :

2. Build .jar : jar cf \[JARNAME\].jar \[PHASE_MAIN_CLASS\] *.class
3. Run : bin/hadoop jar \[JARNAME\].jar TriangleCount \[PATH_TO_DIR\]/data/sample


## TODO :
1. test on the subset of twitter data
2. Redirect all output to S3
3. Deploy on EMR

### Reference
[1] Suri, Siddharth, and Sergei Vassilvitskii. "Counting triangles and the curse of the last reducer." Proceedings of the 20th international conference on World wide web. 2011.
