import os

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

def splitfile(infile_path, size):
    file_name, ext = infile_path.rsplit('.', 1)
    i = 0
    splitting = True
    name_files = []

    with open(infile_path) as infile:
        while splitting:  # while splitting
            outfile_path = "{}{}.{}".format(file_name, i, ext)
            name_files.append(outfile_path)
            with open(outfile_path, 'w') as outfile:
                for line in (infile.readline() for _ in range(size)):
                    outfile.write(line)
                written = bool(line)
            if not written:
                splitting = False
            else:
                i += 1

    return name_files

def split_file(file, maxSize):
    file_size = os.path.getsize(file)
    num_files = (file_size + maxSize - 1) // maxSize

    with open(file, 'rb') as f:
        for i in range(num_files):
            with open(f"{file}_{i}.txt", 'wb') as out:
                written_size = 0
                while written_size < maxSize:
                    chunk = f.readline()
                    if not chunk:
                        break
                    out.write(chunk)
                    written_size += len(chunk)

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

size = 50 * 1024 * 1024
split_file("prueba110mb.txt", size)
'''se le pasa en BYTES al splitfile'''







'''
       pool = multiprocessing.Pool(processes=num_processes)
       res_map = pool.map(map_phase, [names_to_map])
       pool.close()
       pool.join()
       '''
fmap = time.time()
print("MAP DONE " + str(fmap - imap))

# Eliminar archivos
'''if splited:
    for file_to_delete in file_names[0]:
        remove(file_to_delete)
    file_names.clear()

print("FILES DELETED")'''

print("START SHUFFLE")
ishuf = time.time()
# SHUFFLE
res_shuffle = shuffle_phase(res_map[0])
fshuf = time.time()

print("SHUFFLE DONE " + str(fshuf - ishuf))

print("START REDUCE")
ireduce = time.time()
# REDUCE
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
    print(word + " : " + str(round(((res_reduce[0][word] / total_words) * 100), 2)) + "%")
