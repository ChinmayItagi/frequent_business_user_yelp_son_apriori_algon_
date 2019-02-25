import time
import pyspark
import itertools
import json
from pyspark import StorageLevel
sc = pyspark.SparkContext()

def frequentitems(a):

    il = []
    l={}
    l_temp=[]
    #final_list = []
    #generating single items from all baskets in chunk
    chunk = list(a)
    #print("chicnk",chunk)
    for i in chunk:
        for j in i:
            #print('j',j)
            il.append(j)
    # generating counts of all the single items in chunk(candidate singeltons)

    for x in il:
        if x in l.keys():
            l[x]+=1
        else:
            l[x]=1
    #print("dict",l)
    #generating frequent singletons based on threshold
    for k,v in l.items():
        if v>=s_new:
            l_temp.append(k)
    for x in l_temp:

        frequentitemset_final.append(tuple(set([x])))
    #print("single",l_temp)
    #calling a function to generate other combinations from the frequent singletons
    #("chunk",chunk)
    frequentitemset(chunk,l_temp)

    yield frequentitemset_final

def frequentitemset(chunk,frequent_items):
    K=2
    #len_rdd=[]
    len_rdd_set=set()
    issubset_count = {}
    len_rdd_set_1 = set()

    count = 0
    if(K==2):
        #generate pairs
        pair_items = itertools.combinations(frequent_items,2)
        pair = list(pair_items)
        print("cad_pairs",pair)
        Chunk_1 = list(chunk)

        #counts the pair occurance in the chunk(candidate pairs)

        for u in pair:
            for j in Chunk_1:
                print("u,j",u,j)
                if (set(u).issubset(set(j))):
                    if u in issubset_count:
                        issubset_count[u] += 1
                    else:
                        issubset_count[u] = 1
        #print("dict_pair",issubset_count)
        for k, v in issubset_count.items():

            if v >= s_new:
                for i in k:
                    len_rdd_set.add(k) # generating set from pairs (needed for next loop)
        for c in len_rdd_set:
            for h in c:
                len_rdd_set_1.add(h)
            frequentitemset_final.append(tuple(sorted(c)))  # appending selected pairs
        #print("pair",len_rdd_set_1)
    K=3
    if (K > 2):

        # repeating until no values in len_rdd_set
        #while K<=4:
        while len(issubset_count) != 0:
            issubset_count = {}

            itemsets = itertools.combinations(len_rdd_set_1, K)

            item=list(itemsets)
            #print("candi>3", item)
            for i in item:
                for j in chunk:

                    if (set(i).issubset(set(j))):
                        if i in issubset_count:
                            issubset_count[i] += 1
                        else:
                            issubset_count[i] = 1

            len_rdd_set = set()
            #print("issubset",issubset_count)
            for k, v in issubset_count.items():

                if v >= s_new:
                    #print("k",k)
                    for i in k:
                        len_rdd_set_1.add(i)

                    frequentitemset_final.append(tuple(sorted(k)))
            #len_rdd_set_1 = list(len_rdd_set_1)


            K+=1





def map2(candiset):

    counter = {}
    temp_list=[]
    for j in candiset:

        for k in rdd_collect:


            temp = set(k)
            if(set(j).issubset(temp)):
                if j in counter:
                    counter[j] += 1
                else:
                    counter[j] = 1
    #checking for the support over the whole dataset
    for k, v in counter.items():
        #print("k,v",k,v,"\n")

        if v >= support:
            temp_list.append(k)

    yield temp_list








if __name__== '__main__':
    #Reading the file
    thresh=70
    st = time.time()
    rdd = sc.textFile("/home/chinmay/Desktop/small1.csv",2)

    df_bus = sc.textFile('/home/chinmay/Desktop/business.json').map(lambda x: json.loads(x))
    df_bus_1 = df_bus.map(lambda df:(df['business_id'],df['state'])).filter(lambda x:x[1]=='NV')
    df_rev = sc.textFile('/home/chinmay/Desktop/review.json').map(lambda x: json.loads(x))
    df_rev_1 = df_rev.map(lambda df:(df['business_id'],df['user_id']))
    df_final_yelp = df_rev_1.join(df_bus_1).map(lambda x:(x[1][0],x[0])).partitionBy(30)
    header = rdd.first()


    rdd_case_yelp = df_final_yelp.groupByKey().mapValues(list).filter(lambda x:len(x[1])>thresh).map(lambda x:x[1]).cache()
    rdd_case_yelp_collect = rdd_case_yelp.collect()
    # Forming the basket for case 2

    # Defining the support and partition size
    support =50

    case= 1

    frequentitemset_final = []
    # Calculating the modified partition
    #no_of_part = rdd.getNumPartitions()
    no_of_part = df_final_yelp.getNumPartitions()
    s_new = round(support / no_of_part)
    # Forming the basket for case 1
    if case ==1:
        rdd_case_1 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).groupByKey().mapValues(list).map(lambda x: x[1]).cache()
        rdd_collect = rdd_case_1.collect()
        rdd_test_final_reduce_output= rdd_case_yelp.mapPartitions(frequentitems).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y)).map(lambda x: x[0]).mapPartitions(map2).map(lambda x:x).collect()




    elif case==2:
        rdd_case_2 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x:(x[1],x[0])).groupByKey().mapValues(list).map(lambda x: x[1]).cache()
        rdd_collect = rdd_case_2.collect()
        rdd_test_final_reduce_output= rdd_case_2.mapPartitions(frequentitems).flatMap(lambda x:x).map(lambda x:(x,1)).reduceByKey(lambda x,y:(x+y)).map(lambda x: x[0]).mapPartitions(map2).map(lambda x:x).collect()


    #Forming basket for case 2

    #singletons = rdd_test.mapPartitions()

    #rdd_test_dict = rdd_test[0]
    #sorter = sorted(rdd_test_dict, key=len, reverse=True)

    print(rdd_test_final_reduce_output)

    #print(rdd_collect)
    end = time.time()
    print(end - st)