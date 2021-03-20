# Code Snippet 1: Creating RDD in PySpark
# import SparkContext and config
from pyspark import SparkContext
from pyspark import SparkConf

SPARK_MASTER='local'
SPARK_APP_NAME='Word StartingwithS'

conf = SparkConf().setMaster(SPARK_MASTER) \
        .setAppName(SPARK_APP_NAME)
        
sc = SparkContext(conf=conf)

userComment = "I like to visit New York. The public transportation are very reliable. There are many shopping complex and sports actitivies. Shopping is always fun in Manhattan"

listWords = userComment.split(" ")
allwords = sc.parallelize(listWords, 2)

def likewords(words):
    return words.lower().startswith("s")
                                      
filter_words = allwords.filter(lambda word: likewords(word)).collect()

for w in filter_words:
    print(w)
