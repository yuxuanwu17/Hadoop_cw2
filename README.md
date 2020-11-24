## Hadoop

Hadoop comprises three part:
1. Main function, including some basic configuration of job
2. Mapper class, inherited from and override the Map function
3. Reduce class, inherited from Reduce function and override it

### 1. Calculate the bigram frequency and return the Top 10
---
#### data cleaning part (single world)
- Use regular expression to extract the useful information
- For the neat of code, I separate the word cleaning part into one method: deleteNotion
- The idea is to loop all the words and feed into the deleteNotion function to clean the data
- Since the word fed into the program is a txt file, we need first calculate each word by .split("\\s+"), and called String [] single_word
- The regular expression is designed to match all the punctuation and numbers except for English words 
- Note that \W didn't include the number, so we have to make a union of numbers and other punctuation in \W, that is [\\\w[0-9]]
- Create an ArrayList to store the processed data.

#### bigram extraction 
- loop through the single_word [] list
- Note that: since we are calculating the bigram, the length should be length(single_word)-1
- We also need to ensure that the first and last were not empty
- Then add the data to the newly created Arraylist
---
### 2. Return the line containing the word 'torture'
- Use regular expression to separate line and store it in to the list
- The regular expression was written as {\\\n{1,}}, detecting the "\n" and store it as one line 
- Create an arraylist results to each line 
- Loop through the arraylist and use .contain function to find the line contain "torture"
- Note that, the mapper variable and reduce variable should be switched to Text, the attribute problem should take it seriously

---
### Results

#### bigram frequency 
![Alt text](https://github.com/yuxuanwu17/Hadoop_cw2/blob/chenck_method/figure%20/I(base)%20Yuxuan1nput%20hopezhus%20hadoop%20dfs%20-cat%20pa1990utputCM%C2%AE.png)

#### lines containing "torture"
![Alt text](https://github.com/yuxuanwu17/Hadoop_cw2/blob/chenck_method/figure%20/Shuffled%20Maps%20%3D1.png)
