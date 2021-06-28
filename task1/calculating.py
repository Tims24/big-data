import numpy as np
import timeit
import mmap
import os
from multiprocessing import Pool, freeze_support

file_name = 'int32.txt'


def simple(file_name):
    file = open(file_name, 'r+b')
    byte_array = file.read()
    file.close()

    print_results(*perform_map(byte_array))


def multithread(file_name, threads=2):
    file_size = os.path.getsize(file_name)
    chunk_size = file_size // threads
    chunks = [[chunk_size * i, chunk_size * (i + 1)] for i in range(threads)]
    chunks[-1][1] = None

    with open(file_name, 'r+b') as f:
        with mmap.mmap(f.fileno(), 0) as mm:
            with Pool(threads) as pool:
                results = pool.map(perform_map, [mm[chunk[0]: chunk[1]] for chunk in chunks])
            mm.close()

        res_sum = sum(r[0] for r in results)
        res_max = max(r[1] for r in results)
        res_min = min(r[2] for r in results)

        print_results(res_sum, res_max, res_min)


def perform_map(byte_array):
    array = np.frombuffer(byte_array, dtype=np.dtype('uint32').newbyteorder('B'))
    res_sum, res_max, res_min = 0, 0, array[0]

    for num in array:
        if num > res_max:
            res_max = num
        if num < res_min:
            res_min = num
        res_sum += num

    return res_sum, res_max, res_min


def print_results(sum, max, min):
    print(f'Sum: {sum}')
    print(f'Max: {max}')
    print(f'Min: {min}')


if __name__ == '__main__':
    freeze_support()
    print('Простое последовательное чтение')
    print(timeit.timeit('simple(file_name)', globals=locals(), number=1))

    print('Multithread search')
    print(timeit.timeit('multithread(file_name)', globals=ocals(), number=1))
