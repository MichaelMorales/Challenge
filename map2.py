#coding=utf-8
from mrjob.job import MRJob
import re
import pandas as pd
from mr3px.csvprotocol import CsvProtocol


WORD_RE=re.compile(r"[\w']+")

class MRWordFreqCount(MRJob):
    OUTPUT_PROTOCOL = CsvProtocol  # write output as CSV
    def mapper(self, _, line):
        
        for word in WORD_RE.findall(line):
            #print("mapper")
            yield word.lower().replace("á", "a").replace("é", "e").replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n") , 1
    def combiner(self, word, counts):
        yield word, sum(counts)
        palabras[word] = sum(counts)

    def reducer(self, word, counts):
        #print("reducer")
        yield word, sum(counts)
        palabras[word] = sum(counts)
        #datos = pd.DataFrame(data=word)
        #datos.head(10)

if __name__ == '__main__':
    palabras = dict()
    print("Inicio")
    MRWordFreqCount.run()
    print("Fin")
    #print(MRWordFreqCount.run())
    print("Fin2")
    print(palabras)