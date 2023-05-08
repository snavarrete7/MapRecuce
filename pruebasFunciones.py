import os
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

size = 50 * 1024 * 1024
split_file("prueba110mb.txt", size)
'''se le pasa en BYTES al splitfile'''
