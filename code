import sys
import math
import itertools
from pyspark import SparkConf, SparkContext
import time

K = int(sys.argv[1])
support = int(sys.argv[2])
input_file_path = sys.argv[3]
output_file_path = sys.argv[4]

conf = SparkConf().setAppName("inf553_hw2_task2").setMaster("local[*]")
sc = SparkContext(conf=conf)

start = time.time()

def chunk_candidate_frequent(baskets):
    sub_support = math.ceil(len(baskets) / total_item * int(support))

    candidates = []
    # region pass 1
    max_k = 0
    item_count = {}
    bucket_count = [0] * (10 ** 8)
    for basket in baskets:
        basket = basket[1]

        # count the max item in each basket
        if len(basket) > max_k:
            max_k = len(basket)

        # count single item of chunk
        for item in basket:
            item = (item,)
            if item not in item_count:
                item_count[item] = 1
            else:
                item_count[item] = item_count[item] + 1

        # hash all possible pairs
        exist_pair = list(itertools.combinations(sorted(basket), 2))
        for pair in exist_pair:
            bucket_number = (hash(pair[0]) + hash(pair[1])) % (10 ** 8)
            bucket_count[bucket_number] = bucket_count[bucket_number] + 1
    # endregion

    # region pass 2
    # find frequent single items and add frequent single items to candidates
    frequent_items = []
    for item, count in item_count.items():
        if count >= sub_support:
            frequent_items.append(item)
            candidates.append(item)
            candidates=sorted(candidates)
    del item_count

    if len(frequent_items) == 0:
        return []

    # check the pair if hash to the frequent bucket
    candidate_pairs = list(
        itertools.combinations(sorted([itemset[0] for itemset in frequent_items]), 2)
    )
    true_candidate_pairs = []
    for pair in candidate_pairs:
        bucket_number = (hash(pair[0]) + hash(pair[1])) % (10 ** 8)
        if bucket_count[bucket_number] >= sub_support:
            true_candidate_pairs.append(pair)
    del bucket_count
    # count candidate pair
    candidate_pair_cnt = {}
    for basket in baskets:
        basket = basket[1]
        for pair in true_candidate_pairs:
            if set(pair).issubset(basket):
                if pair not in candidate_pair_cnt:
                    candidate_pair_cnt[pair] = 1
                else:
                    candidate_pair_cnt[pair] = candidate_pair_cnt[pair] + 1
    del true_candidate_pairs

    # find frequent pair and add to candidates
    frequent_itemsets = []
    for pair, count in sorted(candidate_pair_cnt.items()):
        if count >= sub_support:
            frequent_itemsets.append(pair)
            candidates.append(pair)
    del candidate_pair_cnt

    if len(frequent_itemsets) == 0:
        return candidates
    # endregion

    # region pass 3 or more
    for k in range(3, max_k + 1):
        # merge subset to generate candidate
        candidate_itemsets = []
        for candidate1 in frequent_itemsets:
            for candidate2 in frequent_itemsets[1:]:
                if (
                    candidate1[:-1] == candidate2[:-1]
                    and candidate1[-1] != candidate2[-1]
                ):
                    merge = set(candidate1).union(set(candidate2))
                    # check subset of merge if are frequent
                    subests = list(itertools.combinations(sorted(merge), k - 1))
                    if set(subests).issubset(set(frequent_itemsets)):
                        candidate_itemsets.append(tuple(sorted(merge)))
                        candidate_itemsets=sorted(candidate_itemsets)
        candidate_itemsets = list(set(candidate_itemsets))
        if candidate_itemsets == []:
            break
        else:
            # count the candidate itemsets
            candidate_itemsets_cnt = {}
            for basket in baskets:
                basket = basket[1]
                for candidate_itemset in candidate_itemsets:
                    if set(candidate_itemset).issubset(basket):
                        if candidate_itemset not in candidate_itemsets_cnt:
                            candidate_itemsets_cnt[candidate_itemset] = 1
                        else:
                            candidate_itemsets_cnt[candidate_itemset] = (
                                candidate_itemsets_cnt[candidate_itemset] + 1
                            )
            # find frequent itemsets and add to the candidates
            frequent_itemsets = []
            for item, count in sorted(candidate_itemsets_cnt.items()):
                if count >= sub_support:
                    frequent_itemsets.append(item)
                    candidates.append(item)
            if len(frequent_itemsets) == 0:
                return candidates
    # endregion
    return candidates

# count all_candidates in each chunk to find frequents
def cnt(partition, all_candidates):
    frequent = []
    for candidate in all_candidates:
        for i in partition:
            if set(candidate) <= set(i[1]):
                frequent.append((candidate, 1))
    return frequent


sc.setLogLevel("WARN")
# user[business11,business12,business13....]
# Candidates
baskets = (
    sc.textFile(input_file_path)
    .filter(lambda line: "user_id,business_id" != line)
    .map(lambda x: tuple(x.split(",")))
    .distinct()
    .groupByKey()
    .mapValues(set)
    .filter(lambda x: len(x[1]) > K)
)
total_item = baskets.count()
all_candidates = (
    baskets.mapPartitions(
        lambda partition: chunk_candidate_frequent(list(partition))
    )
    .distinct()
    .collect()
)

# Frequent Itemsets:
frequents = (
    baskets.mapPartitions(lambda partition: cnt(list(partition), all_candidates))
        .reduceByKey(lambda x, y: x + y)
        .filter(lambda x: x[1] >= support)
        .map(lambda x: x[0])
        .collect()
)


Candidates = {}
for c in all_candidates:
    l = len(c)
    if l not in Candidates:
        Candidates[l] = [c]
    else:
        Candidates[l].append(c)

Frequents = {}
for f in frequents:
    l = len(f)
    if l not in Frequents:
        Frequents[l] = [f]
    else:
        Frequents[l].append(f)

with open(output_file_path, "w") as f:
    f.write("Candidates:\n")
    for k in Candidates.keys():
        if k == 1:
            l1 = (
                str(sorted(Candidates[1]))
                .replace("[(", "(")
                .replace(",), ", "),")
                .replace(",)]", ")")
            )
            f.write(l1 + "\n\r")
        else:
            lk = (
                str(sorted(Candidates[k]))
                .replace("[(", "(")
                .replace("), (", "),(")
                .replace(")]", ")")
            )
            f.write(lk + "\n\r")

    f.write("Frequent Itemsets:\n")
    for k in Frequents.keys():
        if k == 1:
            l1 = (
                str(sorted(Frequents[1]))
                .replace("[(", "(")
                .replace(",), ", "),")
                .replace(",)]", ")")
            )
            f.write(l1 + "\n\r")
        else:
            lk = (
                str(sorted(Frequents[k]))
                .replace("[(", "(")
                .replace("), (", "),(")
                .replace(")]", ")")
            )
            f.write(lk + "\n\r")

end = time.time()
print("Duration: " + str(end - start))
