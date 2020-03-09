#!/usr/bin/env python
# coding: utf-8

# In[81]:


from pyspark import SparkConf, SparkContext
from string import ascii_lowercase, translate, maketrans
from nltk.corpus import words as words_library

# declare spark context if its first time running
# sc = SparkContext("local", "First App")

# takes a list of strings and returns total occurrences, word count
def find_matching_strings(list):
    lower_list = map(lambda x: x.lower, list)
    words_dict = {}
    true_word_count = {}
    occurrences = {}
    for word in list:
        if word.lower() not in words_dict:
            words_dict[word.lower()] = 1
            true_word_count[word] = 1
            occurrences[word.lower()] = 1
        else:
            if word not in true_word_count:
                true_word_count[word] = 1
                occurrences[word.lower()] = occurrences[word.lower()] + 1
            else:
                true_word_count[word] = true_word_count[word] + 1
            words_dict[word.lower()] = words_dict[word.lower()] + 1
    return words_dict

#Used to shift text based on uppercase and lowercase letter of alphabet
def shifttext(text, shift):
    cipher = ''
    for char in text:
        if char == ' ':
          cipher = cipher + char
        elif char.lower() not in ascii_lowercase:
            cipher + char
        elif  char.isupper():
          cipher = cipher + chr((ord(char) + shift - 65) % 26 + 65)
        else:
          cipher = cipher + chr((ord(char) + shift - 97) % 26 + 97)

    return cipher 

def decrypt_text_file(text_file, shift):
    new_words = text_file.map(lambda line: shifttext(line, c))
    print(new_words.take(15))
    new_words.saveAsTextFile('Decrypted-text-3')
        
words = sc.textFile('Encrypted-1.txt')

words_list = words.flatMap(lambda line: line.split(" ")).collect()

count = find_matching_strings(words_list)
total = 0
word = ""
#looks for most used word in document
for key in count.keys():
    if count[key] > total and len(key) > 2:
        word = key
        total = count[key]
        
#runs a decryption test on most used word to find the document shift
for c in range(26):
    shifted_word = shifttext(str(word), c)
    if shifted_word in words_library.words() and shifted_word.lower() != "gur":
        for w in words_list:
            current_word = shifttext(str(w), c)
            #Once document shift is found, run the decryption on the entire document
            if current_word.lower() in words_library.words() and len(current_word) > 4:
                decrypt_text_file(words, c)
                break


# In[ ]:





# In[ ]:




