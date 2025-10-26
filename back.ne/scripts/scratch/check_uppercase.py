import re
import sys

pattern = re.compile(r'(?<!^)(?<![.!?]\s)(\b[A-Z][a-zà-öø-ÿĀ-ſʼ’ʾʿ-]+(?:\s+[A-Z][a-zà-öø-ÿĀ-ſʼ’ʾʿ-]+)*)')

i = 1
with (open(sys.argv[1], "r")) as f:
    for line in f.readlines():
        matches = pattern.findall(line)

        if matches:
            print ("Matched: {} at Line {}".format(matches, i))
        
        i += 1
