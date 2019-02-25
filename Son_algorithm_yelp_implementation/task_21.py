import time
import pyspark
import itertools
sc = pyspark.SparkContext()
import math
def frequentitems(a):

    l={}
    l_temp=[]
    chunk = list(a)
    s_new = math.ceil(support * (len(chunk)/float(number_of_bus)))
    print(s_new)
    for i in chunk:
        for j in i:
            if j in l.keys():
                if l[j] < s_new:
                    l[j] += 1
            elif j not in l.keys():
                l[j] = 1
    for k, v in l.items():
        if v >= s_new:
            l_temp.append(k)
            frequentitemset_final.append(tuple(set([k])))
    K = 2
    len_rdd_set = set()
    issubset_count = {}

    if (K == 2):

        pair_items = itertools.combinations(sorted(l_temp), 2)
        pair = list(pair_items)

        count = 0

        for j in chunk:
            for u in pair:

                if (set(u).issubset(j)):
                    if u in issubset_count:
                        if issubset_count[u] <s_new:
                            issubset_count[u] += 1
                    elif u not in issubset_count:
                        issubset_count[u] = 1
            count = count + 1
            print("LOOP ending",count)
        for k, v in issubset_count.items():

            if v >= s_new:
                len_rdd_set.add(k)
        for c in len_rdd_set:
            frequentitemset_final.append(c)

    K = 3
    if (K > 2):

        while len(issubset_count) != 0:
            issubset_count = {}
            itemsets = []
            print("len", len_rdd_set)
            for i in list(len_rdd_set):
                for j in list(len_rdd_set):
                    tempo = tuple(sorted(set(i).union(set(j))))
                    if (len(tempo) == K):
                        if (tempo not in itemsets):
                            temp_pair = itertools.combinations(tempo, K - 1)
                            temp_pair = list(temp_pair)
                            count_of_pairs = 0
                            for y in temp_pair:
                                if (y in len_rdd_set):
                                    count_of_pairs = count_of_pairs + 1
                            if (count_of_pairs == len(temp_pair)):
                                itemsets.append(tempo)
            print("len_2", len_rdd_set)
            item = itemsets
            len_rdd_set = []
            print("item", item)
            for i in item:
                for j in chunk:
                    if (set(i).issubset(j)):
                        if i in issubset_count:
                            if(issubset_count[i] < s_new):
                                issubset_count[i] += 1
                        elif i not in issubset_count:
                            issubset_count[i] = 1

            len_rdd_set = []
            # print("issub",issubset_count)
            for k, v in issubset_count.items():
                if v >= s_new:
                    print("k,v", k, v)
                    len_rdd_set.append(k)

                    frequentitemset_final.append(k)
            print(issubset_count)
            K += 1
    yield frequentitemset_final






def map2(candiset):
    counter = {}
    temp_list=[]
    for j in candiset:
        for k in rdd_collect:
            temp = set(k)
            if(set(j).issubset(temp)):
                if j in counter and counter[j]< support:
                    counter[j] += 1
                elif j not in counter:
                    counter[j] = 1
    for k, v in counter.items():
        if v >= support:
            temp_list.append(k)
    yield temp_list




if __name__== '__main__':
    #Reading the file
    st = time.time()
    rdd = sc.textFile("/home/chinmay/Desktop/task2_dataset.csv",4)


    def country_partitioner(b):
        return hash(b)


    count = 0

    header = rdd.first()
    support = 50

    case= 1

    frequentitemset_final = []

    if case ==1:
        rdd_case_1 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x:(x[0],x[1]))
        rdd_case_1=rdd_case_1.groupByKey().mapValues(lambda x: set(x)).map(lambda x: x[1]).filter(lambda x:len(set(x))>70).cache()
        print("no _of Part", rdd.getNumPartitions())
        rdd_collect = rdd_case_1.collect()
        number_of_bus = len(rdd_collect)
        rdd_test_final_reduce_output= rdd_case_1.mapPartitions(frequentitems).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y)).map(lambda x: x[0]).cache()
        rdd_test_final_reduce_output=rdd_test_final_reduce_output.mapPartitions(map2).flatMap(lambda x:x).collect()





    print(sorted(rdd_test_final_reduce_output,key=len))

    #print(rdd_collect)
    end = time.time()
    print(end - st)