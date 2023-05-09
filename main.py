import multiprocessing
import os
import re
import time


def split_phase(files):
    proces_name = multiprocessing.current_process().name
    print(proces_name)
    namesList = []

    for file in files:
        stats = os.stat(file)
        file_size = stats.st_size / 1e6

        if file_size >= 100:
            max_size_file = 10 * 1024 * 1024
            names = split_file(file, max_size_file)
            for name in names:
                namesList.append(name)
        else:
            names = split_file(file, 100)
            for name in names:
                namesList.append(name)
            #namesList.append(str(file))

    return namesList


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
    for file in file_names[0]:
        map_words = []
        with open(file, 'r') as f:
            words = f.read().lower().split()
            words_clean = []

            for w in words:
                words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))

            longitud = len(words_clean)
            for i in range(longitud):
                map_words.append((words_clean[i], 1))

            for pair in map_words:
                res_map.append(pair)

    return res_map


def map_word(words):
    lista = []
    longitud = len(words)
    for i in range(longitud):
        lista.append((words[i], 1))
    return lista


if __name__ == '__main__':
    inicio = time.time()

    num_processes = 4
    files = ["ArcTecSw_2023_BigData_Practica_Part1_Sample.txt", "ArcTecSw_2023_BigData_Practica_Part1_Sample - copia.txt"]

    stats = os.stat('prueba110mb.txt')
    file_size = stats.st_size / 1e6
    print("Mb size of the file: " + str(stats.st_size / 1e6))

    pool = multiprocessing.Pool(processes=num_processes)
    file_names = pool.map(split_phase, [files])
    pool.close()
    pool.join()

    for fi in file_names[0]:
        print(fi)

    pool = multiprocessing.Pool(processes=num_processes)
    res_map = pool.map(map_phase, [file_names])
    pool.close()
    pool.join()

    '''
    res_map = []
    pool = multiprocessing.Pool(processes=num_processes)
    for file in file_names[0]:
        map_words = []
        with open(file, 'r') as f:
            words = f.read().lower().split()
            words_clean = []
            for w in words:
                words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))
            map_words = pool.map(map_word, [words_clean])
            for pair in map_words[0]:
                res_map.append(pair)
    pool.close()
    pool.join()'''

    fin = time.time()
    print(res_map[0])
    print(fin - inicio)
