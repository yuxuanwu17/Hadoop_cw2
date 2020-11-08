## Hadoop

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

