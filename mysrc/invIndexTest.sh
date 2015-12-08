#!/bin/bash

export JAVA_HOME=/usr

../bin/hadoop jar InvertedIndex.jar edu.umich.cse.eecs485.InvertedIndex ../dataset/test outputOne
../bin/hadoop jar InvertedIndexPartTwo.jar edu.umich.cse.eecs485.InvertedIndexPartTwo outputOne/ outputTwo
../bin/hadoop jar InvertedIndexPartThree.jar edu.umich.cse.eecs485.InvertedIndexPartThree outputTwo/ output