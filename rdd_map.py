from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName("Map Quiz")
sc = SparkContext.getOrCreate(conf=conf)

file_content = sc.textFile('data/map_quiz.txt')

def get_length(x):
    words = x.split()
    len_list = []
    for word in words:
        len_list.append(len(word))
        
    return len_list

result = file_content.map(get_length)

#result = file_content.map(lambda x:[len(i) for i in x.split()])

print(result.collect())

sc.stop()