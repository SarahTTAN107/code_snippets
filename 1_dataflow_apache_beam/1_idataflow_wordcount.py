# this file is used to reconstruct the code in here: git clone https://github.com/GoogleCloudPlatform/training-data-analyst
# to understand MapReduce better

#import apache_beam as beam
#import sys

def my_grep(line, term):
    if term in line:
        yield line 
        
if __name__ == '__main__':
    paragraph = ("A", "B", "C", "ABC", "AB")

    word = 'A'
    
    for line in paragraph:
        new_word_generator = my_grep(line, word)
        for item in new_word_generator:
            print(new_word_generator)
            print(list(new_word_generator))
            print(item)
        