import os
import glob
import re
import time
import pandas as pd
import concurrent.futures

ROOT = 'C:\\'

PARALLEL = True

EXCLUDE = [
    os.path.join(ROOT, '$Recycle.Bin'),
    os.path.join(ROOT, 'Windows')
]

EXCLUDE_RE = list(map(lambda path: f'{re.escape(path)}(.*)?', EXCLUDE))
EXCLUDE_EXPR = "^(" + "|".join(EXCLUDE_RE) + ")$"

def scan_glob(glob_pattern):
    sizes = dict()
    for i, filepath in enumerate(glob.iglob(glob_pattern, recursive=True)):
        
        # get size
        try:
            size = os.path.getsize(filepath)
        except FileNotFoundError:
            continue

        # increment tree sizes
        pieces = os.path.normpath(filepath).split(os.sep)
        for j, piece in enumerate(pieces):
            _path = os.path.join(*pieces[:j+1])
            sizes[_path] = sizes.get(_path, 0) + size
        
        # log progress
        if i%1000 == 0:
            print(f'Searching files ({glob_pattern}, {i})')
        
        # # stop early for testing
        # if i >= 5000:
        #     print(f'\nEnding on {os.path.split(filepath)} (size: {size}) (index: {i})')
        #     break
        
    return sizes


def main():

    start = time.time()

    # keep track of sizes of items
    glob_sizes = list()

    # partition filesystem in globs
    globs = list()

    # generate globs
    for item in os.listdir(ROOT):
        dirpath = os.path.join(ROOT, item)
        if not re.match(EXCLUDE_EXPR, dirpath):
            _glob = os.path.join(dirpath, '**/*')
            globs.append(_glob)

    if PARALLEL:
        with concurrent.futures.ProcessPoolExecutor(max_workers=6) as executor:
            for size_data in executor.map(scan_glob, globs):
                glob_sizes.append(size_data)
    else:
        for _glob in globs:
            glob_sizes.append(scan_glob(_glob))

    # aggregate glob sizes
    sizes = dict()
    for glob_size in glob_sizes:
        for path, size in glob_size.items():
            sizes[path] = sizes.get(path, 0) + size

    # generate df
    print('Generating dataframe')
    df = pd.DataFrame(sizes.items(), columns=['Path', 'Size'])
    df.sort_values(by='Size', ascending=False, inplace=True)
    df['SizeGB'] = df['Size']/1e9
    df.to_csv('results.csv', index=False)
    print(df.head())

    # max item is C:
    GB = df['SizeGB'].max()
    elapsed = time.time() - start
    print(f'Scanned {round(GB, 2)} GB in {elapsed} seconds')

if __name__ == '__main__':
    main()