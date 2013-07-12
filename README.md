# MRSmallFileCombiner
## Overview
Some times small files land on Hadoop and need to be processes or compacted into larger compressed files.  If you use the default TextInputFormat you will get a mapper per file.  If you have 100,000 files then you have slowness.  This example is how to make the number of mapper configurable and each mapper consumer a equal percentage of files.

The result will also compress the output in sequence files in ether snappy, gzip, bzip2

##Main Class
com.cloudera.sa.ConfigableMapperExample.ManyTxtToFewSeqJob

##How to execute
ManyTxtToFewSeqJob {inputPath} {outputPath} {# mappers} {compressionCodec}

Example: ManyTxtToFewSeqJob ./input ./output 20 snappy

compression codec can be snappy, gzip, or bzip2


