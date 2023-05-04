# This is a sample Python script.

# Press Mayús+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import re

file = "ArcTecSw_2023_BigData_Practica_Part1_Sample.txt";
def split_phase(file, num_lines):
    with open(file) as f:
        text = f.read()
    lines = text.split("\n")
    chunks = []
    for i in range(0, len(lines), num_lines):
        chunk = lines[i:i+num_lines]
        words = []
        for line in chunk:
            words_in_line = re.findall(r'\w+', line.lower())
            words.extend(words_in_line)
        chunks.append(words)
    return chunks

def map_phase(file) :
    return file

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    prova2 = split_phase(file, 28);
    print(prova2)
# See PyCharm help at https://www.jetbrains.com/help/pycharm/

