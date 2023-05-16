import argparse
import multiprocessing
import os
import re
import time
from os import remove

''' Practica 2 ATS - Algoritmo Map-Reduce
    Sergio Navarrete Villalta - 1565742
    Rubén Simó Marín - 1569391      '''

#Funcion para controlar los diferentes tamaños de los archivos
def split_phase(file):
    proces_name = multiprocessing.current_process().name
    print(proces_name)
    namesList = []

    stats = os.stat("/data/"+file)
    file_size = stats.st_size / 1e6                 #Tamaño en MB del archivo

    if 100 <= file_size < 800:
        splited = True
        max_size_file = 10 * 1024 * 1024            #El archivo se dividirá en bloques de 10MB
        names = split_file(file, max_size_file)     #Llamamos a la funcion para dividir los archivos
        for name in names:
            namesList.append(name)
    elif file_size >= 800:
        splited = True
        max_size_file = 100 * 1024 * 1024           #El archivo se dividirá en bloques de 100MB
        names = split_file(file, max_size_file)     #Llamamos a la funcion para dividir los archivos
        for name in names:
            namesList.append(name)
    else:
        splited = False

    if splited == True:                             #Si el arhivo ha sido dividido se devuele una lista con el nombre de las diferentes particiones
        return namesList
    else:
        return None                                 #Si no ha sido dividido devolveremos None


#Funcion para dividir el archivo en diferentes bloques
def split_file(file, maxSize):

    file_size = os.path.getsize("/data/"+file)
    num_files = (file_size + maxSize - 1) // maxSize                     #En funcion del tamaño del archivo y el tamaño maximo de cada bloque se calcula el numero de particiones a generar
    name_file = []

    with open("/data/"+file, 'rb') as f:

        for i in range(num_files):

            with open(f"/data/"+file+"_"+str(i)+".txt", 'wb') as out:    #Abrimos cada bloque a escribir en modo "escritura" y se le asigna un nombre

                file_written_size = 0
                name_file.append(file + "_" + str(i) + ".txt")

                while file_written_size < maxSize:                       #Si el tamaño del bloque es menor al tamaño maximo que se quiere se escribe la siguiente linea del archivo original
                    line = f.readline()
                    if not line:                                         #Controlar si se ha llegado al final del archivo
                        break
                    out.write(line)
                    file_written_size += len(line)                       #Guardar el nuevo tamaño

    return name_file


#Funcion que implementa la fase "map" del algoritmo
def map_phase(file):
    res_map = []

    map_words = []
    with open("/data/"+file, 'r') as f:

        for line in f:
            words = line.lower().split()
            words_clean = []

            for w in words:
                words_clean.append(re.sub(r'[^a-zA-Z0-9]', '', w))

            longitud = len(words_clean)
            for i in range(longitud):
                map_words.append((words_clean[i], 1))       #Para cada palabra de la linea leida y limpiada le asignamos un 1 para crear las tuplas (word, 1)

        for pair in map_words:                              #Nos guardamos cada tupla en una nueva lista de tuplas
            res_map.append(pair)

    return res_map


#Funcion para implementar la fase "shuffle" del algoritmo
def shuffle_phase(mapped_words):
    shuffled_dict = {}
    for word, value in mapped_words:
        if word in shuffled_dict:
            shuffled_dict[word].append(value)
        else:
            shuffled_dict[word] = [value]
    return list(shuffled_dict.items())      #Devolvemos una lista con las tuplas (word, [1,1,1...])


def reduce_words(j):
    res = sum(j)
    return res

#Funcion main que implementara el algoritmo y llamara a las diferentes fases
def mainMapReduce(num_processes, files):

    for file in files:

        #SPLIT FILES PARALIZED
        pool = multiprocessing.Pool(processes=num_processes)
        file_names = pool.map(split_phase, [file])
        pool.close()
        pool.join()

        names_to_map = []
        splited = None
        if file_names[0] is None:       #Si no se ha dividido el archivo original, se añade el nombre el archivo original a la lista de archivos a mapear
            file_names.clear()
            names_to_map.append(file)
            splited = False
        else:                           #Se añade el nombre de los diferentes bloques del archivo original a la lista de archivos a mapear
            splited = True
            for i in file_names[0]:
                names_to_map.append(i)


        finalResult = {}
        for file_to_proces in names_to_map:     #Se realiza el algoritmo map-reduce para cada bloque de la lista de archivos names_to_map

            res_map = []
            file_mapped = []

            #MAP PARALIZED
            pool = multiprocessing.Pool(processes=num_processes)
            file_mapped = pool.map(map_phase, [file_to_proces])
            pool.close()
            pool.join()

            if splited == True:                     # file_to_proces es un bloque (no el arhivo original)
                remove("/data/"+file_to_proces)     # se elimina el bloque

            # SHUFFLE
            res_shuffle = shuffle_phase(file_mapped[0])

            #REDUCE PARALIZED
            res_reduce = {}
            pool = multiprocessing.Pool(processes=num_processes)
            for i, j in res_shuffle:
                res_reduce[i] = pool.map(reduce_words, [j])
            pool.close()
            pool.join()

            #Se unifican los resultados de la fase "reduce" de cada bloque
            for word, count in res_reduce.items():
                if word in finalResult:
                    finalResult[word] += count
                else:
                    finalResult[word] = count

        #Calculo del numero total de palabras
        total_words = 0
        for i in finalResult:
            total_words = sum(finalResult[i]) + total_words
        for i in finalResult:
            finalResult[i] = sum(finalResult[i])

        #Calculo final mas print del resultado
        print(file + ":")
        for i in finalResult:
            print(i + " : " + str(round(((int(finalResult[i]) / total_words) * 100), 2)) + "%")


if __name__ == '__main__':

    arguments = argparse.ArgumentParser(description="Map-Reduce Algorythm")
    arguments.add_argument('files', metavar='file', type=str, nargs='+', help="Add one or more files to process")
    arguments.add_argument('-p', '--processes', type=int, default=2, help="Put the number of processes (default: 2)")
    arguments.add_argument('-o', '--output', type=int, default=2, help="Number of output txt")
    args = arguments.parse_args()

    files = args.files
    num_processes = args.processes

    mainMapReduce(num_processes, files)

