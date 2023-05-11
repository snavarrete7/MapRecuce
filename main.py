import argparse
import multiprocessing
import os
import re
import time
from os import remove

''' Practica 2 ATS - Algoritmo Map-Reduce
    Sergio Navarrete Villalta - 1565742
    Rubén Simó Marín - 1569391      '''

def split_phase(file):
    proces_name = multiprocessing.current_process().name
    print(proces_name)
    namesList = []

    stats = os.stat(file)
    file_size = stats.st_size / 1e6

    if 100 <= file_size < 800:
        splited = True
        max_size_file = 10 * 1024 * 1024
        names = split_file(file, max_size_file)
        for name in names:
            namesList.append(name)
    elif file_size >= 800:
        splited = True
        max_size_file = 100 * 1024 * 1024
        names = split_file(file, max_size_file)
        for name in names:
            namesList.append(name)
    else:
        splited = False

    if splited == True:
        return namesList
    else:
        return None


def split_file(file, maxSize):
    file_size = os.path.getsize(file)
    num_files = (file_size + maxSize - 1) // maxSize
    name_file = []
    with open(file, 'rb') as f:
        for i in range(num_files):
            with open(f"{file}_{i}.txt", 'wb') as out:
                written_size = 0
                name_file.append(file + "_" + str(i) + ".txt")
                while written_size < maxSize:
                    chunk = f.readline()
                    if not chunk:
                        break
                    out.write(chunk)
                    written_size += len(chunk)
    return name_file

''''
def map_phase(file_names):
    res_map = []
    for file in file_names:
        map_words = []
        with open(file, 'r') as f:
            #words = f.read().split().lower()
            map_words = []
            for line in f:
                words = line.lower().split()
                words_clean = []

                for w in words:
                    words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))

                longitud = len(words_clean)
                for i in range(longitud):
                    map_words.append((words_clean[i], 1))

            for pair in map_words:
                res_map.append(pair)
        print("Map done for file:" + file)
        remove(file)
        print(file + " REMOVED")

    return res_map'''

def map_phase(file):
    res_map = []

    map_words = []
    with open(file, 'r') as f:

        for line in f:
            words = line.lower().split()
            words_clean = []

            for w in words:
                words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))

            longitud = len(words_clean)
            for i in range(longitud):
                map_words.append((words_clean[i], 1))

        for pair in map_words:
            res_map.append(pair)
    #print("Map done for file:" + file)


    return res_map

def map_word(words):
    lista = []
    file_mapped = []
    longitud = len(words)
    for i in range(longitud):
        lista.append((words[i], 1))
    for elementMapped in lista[0]:
        file_mapped.append(elementMapped)
    return file_mapped

def shuffle_phase(mapped_words):
    shuffled_dict = {}
    for word, value in mapped_words:
        if word in shuffled_dict:
            shuffled_dict[word].append(value)
        else:
            shuffled_dict[word] = [value]
    return list(shuffled_dict.items())


def reduce_phase(shuffle_words):
    resultReduce = {}
    for i, j in shuffle_words:
        resultReduce[i] = sum(j)
    return resultReduce


def reduce_words(j):
    res = sum(j)
    return res


def mainMapReduce(num_processes, files):

    for file in files:

        isplit = time.time()
        #SPLIT FILES
        pool = multiprocessing.Pool(processes=num_processes)
        file_names = pool.map(split_phase, [file])
        pool.close()
        pool.join()

        names_to_map = []
        splited = None
        if file_names[0] is None:
            file_names.clear()
            names_to_map.append(file)
            splited = False
        else:
            splited = True
            for i in file_names[0]:
                names_to_map.append(i)

        fsplit = time.time()
        print("SPLIT DONE " + str(fsplit - isplit))




        print("START MAP")
        imap = time.time()

        word_count_dict = {}
        for file_to_proces in names_to_map:

            res_map = []
            file_mapped = []
            '''
            pool = multiprocessing.Pool(processes=num_processes)
            with open(file_to_proces, 'r') as f:

                for line in f:
                    line_mapped = []
                    words = line.lower().split()
                    words_clean = []

                    for w in words:
                        words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))

                    line_mapped = pool.map(map_word, [words_clean])



            pool.close()
            pool.join()'''


            pool = multiprocessing.Pool(processes=num_processes)
            file_mapped = pool.map(map_phase, [file_to_proces])
            pool.close()
            pool.join()

            if splited == True:
                remove(file_to_proces)

            # SHUFFLE
            res_shuffle = shuffle_phase(file_mapped[0]) #poner [0] o no segun el map que se quiera utilizar

            #REDUCE
            res_reduce = {}
            pool = multiprocessing.Pool(processes=num_processes)
            for i, j in res_shuffle:
                # dicti[i] = sum(j)
                res_reduce[i] = pool.map(reduce_words, [j])
            pool.close()
            pool.join()



            for word, count in res_reduce.items():
                if word in word_count_dict:
                    word_count_dict[word] += count
                else:
                    word_count_dict[word] = count

            print("Map-Reduce done for file: " + file_to_proces)

        total_words = 0
        for i in word_count_dict:
            total_words = sum(word_count_dict[i]) + total_words
        for i in word_count_dict:
            word_count_dict[i] = sum(word_count_dict[i])

        print("Results for file: " + file)
        for i in word_count_dict:
            print(i + " : " + str(round(((int(word_count_dict[i]) / total_words) * 100), 2)) + "%")


if __name__ == '__main__':
    inicio = time.time()

    arguments = argparse.ArgumentParser(description="Map-Reduce Algorythm")
    arguments.add_argument('files', metavar='file', type=str, nargs='+', help="Add one or more files to process")
    arguments.add_argument('-p', '--processes', type=int, default=2, help="Put the number of processes (default: 2)")
    args = arguments.parse_args()

    files = args.files
    num_processes = args.processes

    start = time.time()
    mainMapReduce(num_processes, files)
    end = time.time()

    print(end - start)

