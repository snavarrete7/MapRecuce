import multiprocessing
import os
import re
import time
from os import remove

def split_phase(file):
    proces_name = multiprocessing.current_process().name
    print(proces_name)
    namesList = []

    stats = os.stat(file)
    file_size = stats.st_size / 1e6

    if file_size >= 100:
        splited = True
        max_size_file = 50 * 1024 * 1024
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


def map_phase(file_names):
    res_map = []
    for file in file_names:
        map_words = []
        with open(file, 'r') as f:
            words = f.read().split().lower()
            words_clean = []

            for w in words:
                words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))

            longitud = len(words_clean)
            for i in range(longitud):
                map_words.append((words_clean[i], 1))

            for pair in map_words:
                res_map.append(pair)
        print("Map done for file:" + file)

    return res_map


def map_word(words):
    lista = []
    longitud = len(words)
    for i in range(longitud):
        lista.append((words[i], 1))
    return lista

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


if __name__ == '__main__':
    inicio = time.time()

    num_processes = 10
    files = ["prueba110mb.txt"]

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
        #MAP
        pool = multiprocessing.Pool(processes=num_processes)
        res_map = pool.map(map_phase, [names_to_map])
        pool.close()
        pool.join()
        fmap = time.time()

        print("MAP DONE " + str(fmap - imap))

        #Eliminar archivos
        if splited:
            for file_to_delete in file_names[0]:
                remove(file_to_delete)
            file_names.clear()

        print("FILES DELETED")

        print("START SHUFFLE")
        ishuf = time.time()
        #SHUFFLE
        res_shuffle = shuffle_phase(res_map[0])
        fshuf = time.time()

        print("SHUFFLE DONE " + str(fshuf - ishuf))

        print("START REDUCE")
        ireduce = time.time()
        #REDUCE
        pool = multiprocessing.Pool(processes=num_processes)
        res_reduce = pool.map(reduce_phase, [res_shuffle])
        pool.close()
        pool.join()
        finreduce = time.time()

        print("REDUCE DONE " + str(finreduce - ireduce))

        '''
        initreduce = time.time()
        dicti = {}
        pool = multiprocessing.Pool(processes=num_processes)
        for i, j in res_shuffle:
            #dicti[i] = sum(j)
            dicti[i] = pool.map(reduce_words, [j])
        pool.close()
        pool.join()
        finreduce = time.time()'''

        total_words = 0
        for i in res_reduce[0]:
            total_words = res_reduce[0][i] + total_words

        print("Results for file: " + file)
        for word in res_reduce[0]:
            print(word + " : " + str(round(((res_reduce[0][word]/total_words)*100), 2)) + "%")

    fin = time.time()
    print(fin - inicio)
