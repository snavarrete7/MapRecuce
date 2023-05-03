# coding=utf-8
"""
Algorythm created by Alejandro Sabater and Albert Gonzalez
"""
import io
import argparse
import sys
import time
from pathlib import Path
import itertools
import gc
import re
from functools import partial
from multiprocessing.pool import Pool

"""

Function to split up the file in little ones. We
receive the name file (path) and the decided size
to be splitted. 

"""


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


"""

Algorithm that selects the letters just 1 time for each word in the line

"""


class MapAlgorithm:

    @staticmethod
    def map_line(line):
        results = []
        target = []
        counter_words = 0

        for word in line.split():
            word = word.lower()
            word = re.sub(r'((?![A-Za-zÀ-ú0-9ა-ჰ一-蠼赋\']+)(\W))|((\W)(?![A-Za-zÀ-ú0-9ა-ჰ一-蠼赋\']+))', '', word)
            counter_words += 1
            results.append((word, 1))
            '''
            if len(word) > 0:
                target.clear()

                for letter in word:
                    if letter.isalpha():
                        found = letter not in target
                        if found:
                            results.append((letter, 1))
                            target.append(letter)'''

        results.append(("Total palabras: ", counter_words))
        return results

    """    
    Function that creates the Pool wich is going to call the function map_line
    with each file and after that will add it to list results
    """

    @staticmethod
    def map(file_to_process):

        with Pool(4) as pool:
            results = pool.map(partial(MapAlgorithm.map_line), file_to_process)
            pool.close()
            pool.join()
        results = list(itertools.chain(*results))

        return [v for v in results if v is not None]

    """

    """

    @staticmethod
    def shuffle(map_result):
        dictionary = {}
        for element in map_result:
            if not element[0] in dictionary:
                dictionary[element[0]] = [element[1]]
            else:
                aux = dictionary.get(element[0])
                aux.append(element[1])
                dictionary[element[0]] = aux

        return dictionary.items()

    """

    """


class ReduceAlgorithm:

    @staticmethod
    def reduce(words):
        dictionary = {}

        for letter in words:
            dictionary[letter[0]] = sum(letter[1])

        gc.collect()  # Deleting memory
        return dictionary


"""
Class TextCounter where the file is processed. Applies the mapper,
shuffle and reduce. 
"""


class TextCounter:

    @staticmethod
    def process_file(file_name):

        with io.open(file_name, 'r', encoding="utf-8") as file_obj:
            mapper_result = MapAlgorithm.map(file_obj)
            shuffle_result = MapAlgorithm.shuffle(mapper_result)
            reducer_result = ReduceAlgorithm.reduce(shuffle_result)

        return file_name, reducer_result

    @staticmethod
    def main(argv):
        file_names = []
        new_files = []
        splited_files = []
        results = []
        results_unified = []
        sub_files = []

        """
        Files are received through arguments and they are reviewed following the steps:
        If the file size is bigger than 100mb it is divided. The maximum size of all the
        files is 100 mbytes.
        """

        parser = argparse.ArgumentParser(description='Map-Reduce Algorythm applied to letters from a file.')
        parser.add_argument('files', metavar='file', nargs='+', help='Add minimum a file.')
        args = parser.parse_args()

        for file in args.files:
            Path(file).stat()
            file_size = Path(file).stat().st_size

            if file_size > 104857600:  # 100 mb = 104857600, 75 mb = 78643200, 50 mb = 52.428.800
                file_names.append(file)  # List with the name of files from arguments to split
                # Max size of 35651584 Kb (1/3 100 Mb) / 4996019 Kb (1/2 Mb),
                # List with the names of subfiles from the original
                splited_files.extend(splitfile(file, 2498010))
                sub_files.append(splitfile(file, 2498010))  # 2498010 Kb for each subfile

            else:
                new_files.append(file)  # List with the name of files without split

        # Case A: The file has not been splitted.
        if new_files:
            for file in new_files:
                results.append(TextCounter.process_file(file))
                results[0]

        # Case B: The file has been splitted. Array with spplited files are
        # all unified in a general array. (results_unified) This is processed
        # in the next step.
        if sub_files:
            for partial_file in splited_files:
                results_unified.append(TextCounter().process_file(partial_file))

        unifying_results = []

        for i in range(len(sub_files)):  # for i in range(len(files)):
            for j in range(len(sub_files[i])):  # for j in range(len(i)):
                unifying_results.append(results_unified[j])  # unifying_results.append(results_unified[j])

            # In this step, all the files which has been already processed are reduced
            # using the same process.
            # for unify in unifying_results:
            # print(file_names)
            word_array = [list(v[1].items()) for v in unifying_results]
            word_list = list(itertools.chain(*word_array))
            shuffler_result = MapAlgorithm.shuffle(word_list)
            results.append([file_names[i], ReduceAlgorithm.reduce(shuffler_result)])
            unifying_results.clear()
            # pos += 1

        # All data in results (singular files and multiple files) are printed on screen.
        if results:
            for result in results:
                print()
                print(result[0] + ":")
                words_number = result[1]["Total palabras: "]
                result[1].pop("Total palabras: ")

                for word, count in result[1].items():
                    final_count = round((int(count) * 100) / int(words_number), 2)
                    print(word, ':', str(final_count) + "%")
                print()


if __name__ == "__main__":
    inicio = time.time()
    TextCounter.main(sys.argv[1:])
    fin = time.time()
    print(fin - inicio)
