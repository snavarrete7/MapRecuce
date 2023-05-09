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
            namesList.append(str(file))

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


def map_phase(file):
    return file


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    inicio = time.time()

    num_processes = 4
    files = ["prueba110mb.txt", "ArcTecSw_2023_BigData_Practica_Part1_Sample.txt"]

    stats = os.stat('prueba110mb.txt')
    file_size = stats.st_size / 1e6
    print("Mb size of the file: " + str(stats.st_size / 1e6))

    pool = multiprocessing.Pool(processes=num_processes)
    file_names = pool.map(split_phase, [files])
    pool.close()
    pool.join()

    for fi in file_names[0]:
        print(fi)

    fin = time.time()
    print(fin - inicio)
