import multiprocessing
import os
import re
import time
'''
def split_phase(file, num_lines):
    with open(file) as f:
        text = f.read()
    lines = text.split("\n")
    chunks = []
    for i in range(0, len(lines), num_lines):
        chunk = lines[i:i + num_lines]
        words = []
        for line in chunk:
            words_in_line = re.findall(r'\w+', line.lower())
            words.extend(words_in_line)
        chunks.append(words)
    return chunks'''


def split_text_by_lines(text, num_lines):
    lines = text.split('\n')
    chunks = [lines[i:i + num_lines] for i in range(0, len(lines), num_lines)]
    for i, chunk in enumerate(chunks):
        with open(f'file_{i}.txt', 'w') as f:
            f.write('\n'.join(chunk))
    return chunks


def split_phase(files):
    proces_name = multiprocessing.current_process().name
    print(proces_name)
    counter = 0
    max_size_file = 0
    nameFilesSplitted = []
    for file in files:
        stats = os.stat(file)
        file_size = stats.st_size / 1e6
        if file_size <= 10:
            max_size_file = 1.0
        elif file_size <= 52:
            max_size_file = 5.0
        elif file_size <= 102:
            max_size_file = 10.0

        counter = split_text(file, max_size_file, counter)
        counter += 1

    for i in range(counter):
        nameFilesSplitted.append("file_" + str(i) + ".txt")
    print(nameFilesSplitted)
    return nameFilesSplitted

def split_text(file, maxSize, counter):
    with open(file, "r") as f:
        current_output_size = 0
        current_output_file = os.path.join(f"file_{counter}.txt")

        line = f.readline()
        while line:

            with open(current_output_file, "a") as output_f:
                output_f.write(line)

            current_output_size += len(line.encode("utf-8"))

            if current_output_size > maxSize * 1024 * 1024:
                counter += 1
                current_output_file = os.path.join(f"file_{counter}.txt")
                current_output_size = 0

            line = f.readline()
    return counter


def map_phase(file):
    return file

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    inicio = time.time()

    num_processes = 8
    files = ["prueba9.txt"]

    stats = os.stat('ArcTecSw_2023_BigData_Practica_Part1_Sample.txt')
    file_size = stats.st_size / 1e6
    print("Mb size of the file: " + str(stats.st_size / 1e6))

    split_phase(files)
    '''
    pool = multiprocessing.Pool(processes=num_processes)
    pool.map(split_phase, [files])
    pool.close()
    pool.join()'''
    fin = time.time()
    print(fin - inicio)






