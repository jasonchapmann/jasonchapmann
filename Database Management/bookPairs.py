from pyspark import SparkContext
sc = SparkContext("local", "bookPairs")

#lines = sc.textFile("goodreads.3000")
lines = sc.textFile("/home/cs143/data/goodreads.dat")
import re
def f(x):
    res = []
    user = re.split(":|,",x)
    books = user[1:]
    book_nums = list(map(int, books))
    book_nums.sort()
    a = len(book_nums)
    for x in range(a):
      for y in range(a):
        if y>x:
          res.append((book_nums[x],book_nums[y]))
    return res

pairs = lines.flatMap(lambda x: f(x))
pairs = pairs.map(lambda pair: (pair, 1))
books = pairs.reduceByKey(lambda a, b: a+b)
book_count = books.filter(lambda count: count[1] > 20)
#book_count.saveAsTextFile("output")
book_count.saveAsTextFile("/home/cs143/output1")