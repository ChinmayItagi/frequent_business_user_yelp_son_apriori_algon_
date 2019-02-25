import time
import pyspark
import itertools
import sys
from pyspark import StorageLevel
sc = pyspark.SparkContext()

def frequentitems(a):
    il = []
    l={}
    l_temp=[]

    #final_list = []
    #generating single items from all baskets in chunk
    chunk = list(a)
    s_new = support * (len(chunk)/float(number_of_bus))
    print(s_new)
    for i in chunk:
        for j in i:
            if j in l.keys() and l[j] < s_new:
                l[j] += 1
            elif j not in l.keys():
                l[j] = 1


    for k, v in l.items():
        if v >= s_new:
            l_temp.append(k)
            frequentitemset_final.append(tuple(set([k])))
    print(len(l_temp))
    frequentitemset(chunk, l_temp, s_new)


    yield frequentitemset_final


def frequentitemset(chunk,frequent_items,s_new):
    K=2
    #len_rdd=[]
    len_rdd_set=set()
    issubset_count = {}
    len_rdd_set_1 = set()


    if(K==2):
        #generate pairs
        pair_items = itertools.combinations(sorted(frequent_items),2)
        pair = list(pair_items)
        #print("pairs",len(pair))
        #Chunk_1 = list(chunk)
        count =0
        #counts the pair occurance in the chunk(candidate pairs)
        for j in chunk:
            for u in pair:
            #for j in Chunk_1:
            #print
                if (set(u).issubset(j)):
                    if u in issubset_count and issubset_count[u]<s_new:
                        issubset_count[u] += 1
                    elif u not in issubset_count:
                        issubset_count[u] = 1
            count = count+1
            print("Loopinng DONE" , count)
        print("is_subset",issubset_count)
        for k, v in issubset_count.items():

            if v >= s_new:

                len_rdd_set.add(k) # generating set from pairs (needed for next loop)
        for c in len_rdd_set:

            frequentitemset_final.append(c)  # appending selected pairs

    K=3
    if (K > 2):

        # repeating until no values in len_rdd_set
        #while K<=4:

        while len(issubset_count) != 0:

            issubset_count = {}
            #itemsets_raw= []
            itemsets=[]
            print("len",len_rdd_set)
            for i in list(len_rdd_set):

                for j in list(len_rdd_set):
                        tempo = tuple(sorted(set(i).union(set(j))))
                        if (len(tempo)==K and (tempo not in itemsets)):
                            temp_pair = itertools.combinations(tempo,K-1)
                            temp_pair = list(temp_pair)
                            count_of_pairs =0
                            for y in temp_pair:
                                if(y in len_rdd_set):
                                    count_of_pairs = count_of_pairs +1
                            if(count_of_pairs==len(temp_pair)):
                                itemsets.append(tempo)
            print("len_2",len_rdd_set)
            item=itemsets
            #len_rdd_set = []
            #print("item",item)
            for i in item:
                for j in chunk:
                    if (set(i).issubset(j)):
                        if i in issubset_count and issubset_count[i]< s_new:
                            issubset_count[i] += 1
                        elif i not in issubset_count:
                            issubset_count[i] = 1
                        #elif issubset_count[i] >=s_new:
                         #   len_rdd_set.append(i)
                          #  frequentitemset_final.append(i)
            #print(issubset_count)

            len_rdd_set = []
            #print("issub",issubset_count)
            for k, v in issubset_count.items():
                if v >= s_new:
                    print("k,v",k,v)
                    len_rdd_set.append(k)

                    frequentitemset_final.append(k)



                #len_rdd_set_1 = list(len_rdd_set_1)
            K+=1



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

    #checking for the support over the whole dataset

    for k, v in counter.items():

        if v >= support:
            temp_list.append(k)


    yield temp_list



if __name__== '__main__':
    #Reading the file
    st = time.time()
    rdd = sc.textFile("/home/chinmay/Desktop/small2.csv")
    header = rdd.first()

    support = 4

    case= 1
    a=0
    frequentitemset_final = []


    if case ==1:
        rdd_case_1 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x:(x[0],x[1])).groupByKey().mapValues(set).map(lambda x: x[1]).cache()
        rdd_collect = rdd_case_1.collect()
        number_of_bus = len(rdd_collect)
        rdd_test_final_reduce_output= rdd_case_1.mapPartitions(frequentitems).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y)).map(lambda x: x[0]).mapPartitions(map2).flatMap(lambda x:x).collect()
        #rdd_test_final_reduce_output = rdd_case_1.mapPartitions(frequentitems).flatMap(lambda x: x).map(lambda x: (x, 1)).reduceByKey(lambda x, y: (x + y)).map(lambda x: x[0]).mapPartitions(phase2).flatMap(lambda x:x).filter(lambda x:x[1]>=support ).map(lambda x:x[0]).collect()
        a = max(len(elem) for elem in rdd_test_final_reduce_output)


    elif case==2:
        rdd_case_2 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x:(x[1],x[0])).groupByKey().mapValues(set).map(lambda x: x[1]).cache()
        rdd_collect = rdd_case_2.collect()
        number_of_bus = len(rdd_collect)
        rdd_test_final_reduce_output= rdd_case_2.mapPartitions(frequentitems).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y)).map(lambda x: x[0]).mapPartitions(map2).flatMap(lambda x:x).collect()




    print( print(sorted(rdd_test_final_reduce_output,key=len)))

    print(a)
    end = time.time()
    print(end - st)