#Tiny Google

## Disclaimer
This project was written in a single week (and finals week, at that). Please excuse any lack of comments or untidy code. 

## Description
This is the final project for CS1699: Cloud Computing. 
It reads in text files and creates an inverted index using either MapReduce or Spark. 
The UI then enables the user to select between the inverted indexes and retrieve ranked results for specific query terms. 

## Usage
### UI
tinyGoogle.py runs the UI where the user can select an index and retrieve results. Currently both indexes should be included in the repo, so it should run just fine without needing to re-run the hadoop or spark indexers. 
### Indexing
The MapReduce indexer is written in java and contained in the mapreduce folder. 
The Spark indexer is written in scala and contained in the spark/src folder. 
Both implementations were tested and run on local installations and require the removal of the contents of the output folder before running. 

## Dependencies
TinyGoogle requires hadoop and spark installations, though they do not have to be local. The scala (spark) portion also uses sbt to build. 
