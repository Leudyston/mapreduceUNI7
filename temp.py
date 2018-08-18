from mrjob.job import MRJob

class MRWordFrequency(MRJob):
  def mapper(self, _, line):
      (location, _, type, value, _, _, _, _) = line.split(",")
      if type in ['TMAX','TMIN']:
          yield location, int(value)
	  
  def reducer(self, location, values):
      temp= []
      for value in values:
         temp.append(value)
      max_temp = max(temp)
      min_temp = min(temp)
      yield location, (min_temp,max_temp)
 

if __name__ == '__main__':
      MRWordFrequency.run()
      
      
      # -*- coding: utf-8 -*-
"""
Spyder Editor

Este é um arquivo de script temporário.
"""

