import time
import pyspark
import itertools
import sys
import math
st = time.time()
sc = pyspark.SparkContext()

def frequentitems(a):
    il = []
    l={}
    l_temp=[]
    #final_list = []
    #generating single items from all baskets in chunk
    chunk = list(a)
    #print("orinal chunk",chunk)
    for i in chunk:
        for j in i:

            il.append(j)
    # generating counts of all the single items in chunk(candidate singeltons)

    for x in il:
        if x in l.keys():
            l[x]+=1
        else:
            l[x]=1

    s_new =math.ceil(support * float(len(chunk)) / float(num_baskets))
    #generating frequent singletons based on threshold
    #print("l", s_new)
    for k,v in l.items():
        if v>=s_new:
            l_temp.append(k)
    for k in sorted(l_temp):
        frequentitemset_final.append((k,1))
    #print("singleton :",frequentitemset_final)
    #calling a function to generate other combinations from the frequent singletons
    #print("chunk",chunk)
    generate_pairs(chunk,l_temp,s_new)

    yield frequentitemset_final

def generate_pairs(chunk,frequent_items,s_new):
    K=2
    #len_rdd=[]
    len_rdd_set=set()
    issubset_count = {}
    #len_rdd_set_1 = set()
    #print("---------------",s_new)
    count = 0
    if(K==2):
        #generate pairs
        pair_items = itertools.combinations(sorted(frequent_items),2)
        pair = list(pair_items) #no duplicates
        Chunk_1 = list(chunk)
        #print("Chunk_1",Chunk_1)
        #counts the pair occurance in the chunk(candidate pairs)
        for j in Chunk_1:
            for u in pair:
            #print("i", u)

                #print("j",j)
                if (set(u).issubset(set(j))):
                    if u in issubset_count:
                        issubset_count[u] += 1
                    else:
                        issubset_count[u] = 1
        #print("dict:",issubset_count)
        for k, v in issubset_count.items():
            #print("k", k)
            if v >= s_new:
                #for i in k:
                len_rdd_set.add(k) # generating set of pairs (needed for next loop)
        #print("ll",len_rdd_set)
        for c in len_rdd_set:
            #print("c",c)
            #for h in c:
            #    len_rdd_set_1.add(h)
            frequentitemset_final.append((c,1))  # appending selected pairs

    K=3
    #print("pairs:", frequentitemset_final)
    #print ("len_rdd_3",len_rdd_set_1)
    if (K > 2):

        # repeating until no values in len_rdd_set
        while len(issubset_count) != 0:
            issubset_count = {} #creating a empty dict every time
            itemsets=[]
            #itemsets = itertools.combinations(sorted(len_rdd_set_1), K)
            for i in len_rdd_set:
                for j in len_rdd_set:
                    #print("gggg",i,j)
                    x = tuple(sorted(set(i + j)))
                    #print("xx",len(x),k)
                    if len(x) == K and x not in itemsets:
                        #print("enteres")
                        #if x not in itemsets:
                        itemsets.append(x)
            #item=list(itemsets)
            for j in chunk:
                for i in itemsets:
                    #print("iii,jjj",i,j)
                    if (set(i).issubset(set(j))):
                        if i in issubset_count:
                            issubset_count[i] += 1
                        else:
                            issubset_count[i] = 1
            #print("dict_3",issubset_count)
            len_rdd_set = []
            if issubset_count!=[]:
                for k, v in issubset_count.items():

                    if v >= s_new:
                        #print("k,v",k,v)
                        if k not in len_rdd_set:
                            len_rdd_set.append(sorted(k))
                        if k not in frequentitemset_final:
                            frequentitemset_final.append((k,1))
            K+=1

def phase2(candidates):

    frequentitemset_final_p2=[]
    freq_itemset_p2={}
    #c=tuple(candidates)
    print("cc",candidates)
    key = candidates[0]
    a=[]
    a.append(1)
    for j in rdd_collect:
        if(isinstance(key,tuple)==False):
            #print(type(key))
            #print("----------")
            if key in j:
                if key in freq_itemset_p2:
                    freq_itemset_p2[key] += 1
                else:
                    freq_itemset_p2[key] = 1
        else:
            #print("key set",set(key))
            if(set(key).issubset(set(j))):
                if key in freq_itemset_p2:
                    freq_itemset_p2[key]+=1
                else:
                    freq_itemset_p2[key]=1
    print("yaay",freq_itemset_p2)
    for k,v in freq_itemset_p2.items():
        frequentitemset_final_p2.append((k,v))
    print("appended list ",frequentitemset_final_p2)

    return  frequentitemset_final_p2
if __name__== '__main__':
    #Reading the file

    rdd = sc.textFile("/home/chinmay/Desktop/small2.csv",4)
    header = rdd.first()

    support=9
    p_size=2

    frequentitemset_final = []
    #frequentitemset_final_p2 = []
    no_of_part = rdd.getNumPartitions()
    #s_new = round(support / no_of_part)
    case=2
    rdd1=rdd.filter(lambda x:x!=header).map(lambda x:x.split(",")).groupByKey().cache()
    num_baskets=rdd1.count()

    if case==1:
        # Forming the baskets
        rdd_2 = rdd.filter(lambda x:x!=header).map(lambda x:x.split(",")).map(lambda x:(int(x[0]),int(x[1]))).groupByKey().mapValues(list).map(lambda x:x[1]).cache()
        rdd_collect=rdd_2.collect()

        rdd_test = rdd_2.mapPartitions(frequentitems).flatMap(lambda x:x).reduceByKey(lambda x,y:x+y).map(phase2).flatMap(lambda x:x).filter(lambda x:x[1]>=support ).map(lambda x:x[0]).collect()

    elif case==2:
        rdd_2 = rdd.filter(lambda x: x != header).map(lambda x: x.split(",")).map(lambda x:(int(x[1]),int(x[0]))).groupByKey().mapValues(list).map(lambda x: x[1]).cache()
        rdd_collect = rdd_2.collect()

        rdd_test = rdd_2.mapPartitions(frequentitems).flatMap(lambda x: x).reduceByKey(lambda x, y: x + y).map(phase2).flatMap(lambda x: x).filter(lambda x: x[1] >= support).map(lambda x: x[0]).collect()

    print("final",rdd_test)
    #rdd_new=[]
    #for i in rdd_test:
        #if (isinstance(i,tuple)==False):
        #    i=tuple([i])
        #rdd_new.append(i)
    #print(rdd_new)
    #for i in rdd_new:
    #a = max(len(elem) for elem in rdd_new)
#print(a)
end=time.time()
print(end - st)