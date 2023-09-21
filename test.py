import multiprocessing

def sq(n):
    return n * n

if __name__ == '__main__':
    numbers = list(range(10))

    with multiprocessing.Pool() as pool:
        result_pool = pool.map(sq,numbers)
        print("pool",result_pool)
    
    process_l = []
    for n in numbers:
        process = multiprocessing.Process(target=sq, args=(n,))
        process_l.append(process)
        process.start()
    for process in process_l:
        process.join()
    process_r = [p.exitcode for p in process_l]

    print("processl",process_l)
    print("processr",process_r)