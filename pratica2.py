from mrjob.job import MRJob

class MRWordFrequency(MRJob):
  def mapper(self, _, line):
      words = line.split(",")
      
      yield words[2],int(words[3])
	  
  def reducer(self, word, values):
      x = 0
      y = 0
      for value in values:
        x = x + value
        y = y + 1
      yield word,round((x/y),2)
 

if __name__ == '__main__':
      MRWordFrequency.run()