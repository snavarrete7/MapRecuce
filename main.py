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
        max_size_file = 10 * 1024 * 1024
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
    files = ["prueba110mb.txt","ArcTecSw_2023_BigData_Practica_Part1_Sample.txt"]

    for file in files:

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


        pool = multiprocessing.Pool(processes=num_processes)
        res_map = pool.map(map_phase, [names_to_map])
        pool.close()
        pool.join()

        #Eliminar archivos
        if splited:
            for file_to_delete in file_names[0]:
                remove(file_to_delete)
            file_names.clear()

        fin = time.time()
        print("Map result for file: " + str(file))
        print("Number of words: " + str(len(res_map[0])))
        print(res_map[0])
    print(fin - inicio)
