from mrjob.job import MRJob

class MRWordFrequency(MRJob):
  def mapper(self, _, line):
      words = line.split(",")
      
      yield words[0],float(words[2])
	  
  def reducer(self, word, values):
        yield word, round(sum(values),2)
 

if __name__ == '__main__':
      MRWordFrequency.run()