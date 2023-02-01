# Common utility functions
import os
import sys
import io
import difflib

# +/- denotes the diff
# DIFF 3:  lastModifiedDate:2021-05-28T12:00:00-04:00
# DIFF 3:  size:1208531
# DIFF 3:  zipSize:95888
# DIFF 3: -gzSize:95748 - 2nd file do not have this line
# DIFF 3:  sha256:2C345B45E49F6A5B751CB1ACC0EACD7F15B100A3D6E8769813280D7713896B73
# DIFF 3: +some text here - 2nd file has this line
def compareFiles(file1path: str, file2path: str):
    """

    """
    print("compareFiles - ", file1path, file2path)    
    isdiff = False
    with open(file1path, 'r') as f1:
        with open(file2path, 'r') as f2:
            diff = difflib.unified_diff(
                f1.readlines(),
                f2.readlines(),
                fromfile=file1path,
                tofile=file2path,
            )
            for line in diff:
                if line[0] == '+' or line[0] == '-':
                    print("DIFF :", line)
                    isdiff = True

    return isdiff