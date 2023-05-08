import multiprocessing
from collections import Counter
import os
import sys
import re
import string
from itertools import islice
from multiprocessing import Pool, Manager

import re
from multiprocessing import Pool, Manager


def split_text_by_lines(text, num_lines):
    lines = text.split('\n')
    chunks = [lines[i:i + num_lines] for i in range(0, len(lines), num_lines)]
    for i, chunk in enumerate(chunks):
        with open(f'chunk_{i}.txt', 'w') as f:
            f.write('\n'.join(chunk))
    return chunks


def map_words_to_ones(words):
    proces_name = multiprocessing.current_process().name
    print(proces_name)
    return [(word, 1) for word in words]


def shuffle_words(mapped_words):
    shuffled_dict = {}
    for word, value in mapped_words:
        if word in shuffled_dict:
            shuffled_dict[word].append(value)
        else:
            shuffled_dict[word] = [value]
    return list(shuffled_dict.items())


def reduce_words(word_count_pairs):
    word_count_dict = Manager().dict()
    pool = Pool()
    for word, counts in word_count_pairs:
        count_sum = pool.apply(sum, (counts,))
        word_count_dict.update({word: count_sum})
    pool.close()
    pool.join()
    return word_count_dict


def reduce_words2(j):
    res = sum(j)
    return res


if __name__ == '__main__':
    num_lines = 1000
    num_processes = 20

    stats = os.stat('ArcTecSw_2023_BigData_Practica_Part1_Sample.txt')
    print("Mb size of the file: " + str(stats.st_size / 1e6))

    with open("ArcTecSw_2023_BigData_Practica_Part1_Sample.txt") as file:
        text = file.read()

    words_chunks = split_text_by_lines(text, num_lines)

    pool = Pool(processes=num_processes)
    mapped_words = []
    for i in range(len(words_chunks)):
        with open(f'chunk_{i}.txt', 'r') as f:
            words = f.read()
            mapped_words.append(pool.apply_async(map_words_to_ones, args=(re.findall(r'\w+', words.lower()),)))
            '''words = f.read().split()'''
            '''mapped_words.append(pool.map(map_words_to_ones, [words]))'''

    pool.close()
    pool.join()

    shuffled_words = shuffle_words([word for words in [chunk.get() for chunk in mapped_words] for word in words])

    '''Reduce improvisado'''
    dicti = {}
    pool = Pool(processes=num_processes)
    for i, j in shuffled_words:
        '''dicti[i] = sum(j)'''
        dicti[i] = pool.map(reduce_words2, [j])
    pool.close()
    pool.join()

    '''
    pool = Pool(processes=num_processes)
    word_count_pairs = pool.map(reduce_words, [shuffled_words])
    pool.close()
    pool.join()

    word_count_dict = {}
    for pairs_dict in word_count_pairs:
        for word, count in pairs_dict.items():
            if word in word_count_dict:
                word_count_dict[word] += count
            else:
                word_count_dict[word] = count

    total_words = sum(word_count_dict.values())
    word_percentages = {word: (count/total_words)*100 for word, count in word_count_dict.items()}
    print(word_percentages)'''

    '''total_words = sum(dicti.values())'''

    total_words = 0
    for i in dicti:
        total_words = dicti[i][0] + total_words
    ''''word_percentages = {word: (count / total_words) * 100 for word, count in dicti.items()}'''
    word_percentages = {word: (count[0] / total_words) * 100 for word, count in dicti.items()}
    print(word_percentages)
