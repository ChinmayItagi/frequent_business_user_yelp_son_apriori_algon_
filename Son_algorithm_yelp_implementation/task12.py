import time
import pyspark
import itertools
import sys
from pyspark import StorageLevel
sc = pyspark.SparkContext()
import json

thresh = 70

df_bus = sc.textFile('/home/chinmay/Desktop/business.json').map(lambda x: json.loads(x))
df_bus_1 = df_bus.map(lambda df:(df['business_id'],df['categories'],df['name'],df['address'],df['postal_code']))\
.filter(lambda x:x[0]=='RESDUcs7fIiihp38-d6_6g' or x[0]=='K7lWdNUhCbcnEvI0NhGewg' or x[0]=='iCQpiavjjPzJ5_3gPD5Ebg').collect()
print(df_bus_1)


#JyxHvtj-syke7m9rbza7mA', 'UPIYuRaZvknINOd1w8kqRQ', 'j5nPiTwWEFr-VsePew7Sjg'



#'4k3RlMAMd46DZ_JyZU0lMg', 'JyxHvtj-syke7m9rbza7mA', 'UPIYuRaZvknINOd1w8kqRQ'

#'7sPNbCx7vGAaH7SbNPZ6oA', 'JyxHvtj-syke7m9rbza7mA', 'UPIYuRaZvknINOd1w8kqRQ'
#'A5Rkh7UymKm0_Rxm9K2PJw', 'BxKe9Xt_fN6qBzrTofHuEQ', 'FaHADZARwnY4yvlvpnsfGA'


#K7lWdNUhCbcnEvI0NhGewg', 'RESDUcs7fIiihp38-d6_6g', 'iCQpiavjjPzJ5_3gPD5Ebg'
"""""
df_rev = sc.textFile('/home/chinmay/Desktop/review.json').map(lambda x: json.loads(x))
df_rev_1 = df_rev.map(lambda df :(df['business_id'] ,df['user_id'])).cache()
df_final_yelp = df_rev_1.join(df_bus_1).map(lambda x :[x[1][0] ,x[0]])
df_final_yelp_1 = df_final_yelp.collect()
df_final_yelp_mapping = df_final_yelp.map(lambda x:x[1]).distinct().collect()
print(df_final_yelp_mapping)
dict1 = {}
for i,v in enumerate(df_final_yelp_mapping,1):
    dict1[v]=i

for i in df_final_yelp_1:
    value = dict1[i[1]]
    i[1]=value
print(df_final_yelp_1)


#rdd_case_yelp = df_final_yelp.groupByKey().mapValues(list).filter(lambda x: len(x[1]) > thresh).map(lambda x: x[1]).collect()
#print(df_final_yelp)
#rdd_case_yelp_collect = rdd_case_yelp.collect()
with open("yelp_csv.csv",'w') as f:
    f.write("User_id,business_id\n")
    for i in df_final_yelp_1:
        f.write(i[0]+","+str(i[1])+"\n")

"""""