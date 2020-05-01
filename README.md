# Mine Yelp's big data set for frequently reviewed businesses(PySpark)
## Task 
Implement the SON algorithm to solve all tasks on top fo Apache Spark Framework. I find all the possible combinations 
of frequent itemsets in any input file within the required time
### Task1: simulated data
#### Built two kinds of market-basket model:
##### case1:
1. I create a basket for each
user containing the business ids reviewed by this user. If a business was reviewed more than once by a reviewer, we consider this product was rated only once. More specifically, the business ids within each basket are unique.

The generated baskets are similar to:
user1: [business11, business12, business13, ...]
user2: [business21, business22, business23, ...]
user3: [business31, business32, business33, ...] 

2. I calculate the combinations of frequent businesses (as singletons, pairs, triples, etc.)
that are qualified as frequent given a support threshold.

##### case2:
1. I create a basket for each business containing the user ids that commented on this business. Similar to case 1, the userids within each basket are unique. 

The generated baskets are similar to:
business1: [user11, user12, user13, ...]
business2: [user21, user22, user23, ...]
business3: [user31, user32, user33, ...] 

2. I calculate the combinations of frequent users (as singletons, pairs, triples, etc.) that
are qualified as frequent given a support threshold. 

#### Input format:
1. Case number: Integer that specifies the case. 1 for Case 1 and 2 for Case 2.
2. Support: Integer that defines the minimum count to qualify as a frequent itemset.
3. Input file path: This is the path to the input file including path, file name and extension.
4. Output file path: This is the path to the output file including path, file name and extension. 
#### Output file:
##### (1) Output-1
You should use “Candidates:”as the tag. For each line you should output the candidates of
frequent itemsets you find after the first pass of SON algorithm, followed by an empty line
after each fequent-X itemset combination list. The printed itemsets must be sorted in
lexicographical order. (Both user_id and business_id have the data type “string”.)
##### (2) Output-2
You should use “Frequent Itemsets:”as the tag. For each line you should output the final
frequent itemsets you found after finishing the SON algorithm. The format is the same with
the Output-1. The printed itemsets must be sorted in lexicographical order. 
#### Execution example:
spark-submit task1.py <case number> <support> <input_file_path> <output_file_path> 

### Task2: Yelp dataset
I will explore the Yelp dataset to find the frequent business sets (only case 1). I will jointly use the business.json and review.json to generate the output user_business.csv file
#### (1) Data preprocessing
generate a sample dataset from business.json and review.json (https://drive.google.com/drive/folders/1-Y4H0vw2rRIjByDdGcsEuor9VagDyzin?usp=sharing) with
following steps:
1. The state of the business you need is Nevada, i.e., filtering ‘state’== ‘NV’.
2. Select “user_id” and “business_id” from review.json whose “business_id” is from Nevada. Each line in the CSV file would be “user_id1, business_id1”.
3. The header of CSV file should be “user_id,business_id”. I need to save the dataset in CSV format. 
#### (2) Apply SON algorithm
The requirements for task 2 are similar to task 1. However, I will test your implementation
with the large dataset I just generated. For this execution time, the time from reading the file till writing the results to the output file(<2,000 sec). I just find the frequent business sets
(only case 1) from the file I just generated. 
The following are the steps you need to do:
1. Reading the user_business CSV file in to RDD and then build the case 1 market-basket
model;
2. Find out qualified users who reviewed more than k businesses. (k is the filter threshold);
3. Apply the SON algorithm code to the filtered market-basket model;
#### Input format:
1. Filter threshold: Integer that is used to filter out qualified users (70)
2. Support: Integer that defines the minimum count to qualify as a frequent itemset. (50)
3. Input file path: This is the path to the input file including path, file name and extension. (user_business.csv)
4. Output file path: This is the path to the output file including path, file name and extension. (task2.txt)
#### Execution example:
spark-submit task2.py <filter threshold> <support> <input_file_path> <output_file_path> 
