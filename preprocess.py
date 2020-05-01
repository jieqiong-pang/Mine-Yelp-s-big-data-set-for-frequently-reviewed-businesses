from pyspark import SparkConf, SparkContext
import json
import csv

conf = SparkConf().setAppName("inf553_task2").setMaster("local[*]")
sc = SparkContext(conf=conf)

review = sc.textFile('review.json')
j_d_review = (
    review.map(json.loads)
    .map(lambda r: (r["business_id"], r["user_id"]))
)

business = sc.textFile('business.json')
j_d_business = (
    business.map(json.loads)
    .filter(lambda s: s["state"] == "NV")
    .map(lambda b: (b["business_id"],b["state"]))
)

review_join_business = j_d_review.join(j_d_business).map(lambda x:(x[1][0],x[0])).collect()

with open('user_business.csv','w') as f:
    out = csv.writer(f)
    out.writerow(['user_id', 'business_id'])
    for row in review_join_business:
        out.writerow(row)


