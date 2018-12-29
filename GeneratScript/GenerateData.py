import random
import csv


cnt =0 
username = "usr"
techname = "Tech"
tuturl = "abc.com"
rating = 0
tags = "oop"

fieldnames = ['userID', 'tutID','rating']

while True:
	cnt = cnt + 1
	tutrandom = random.randint(1,101)
	lst = list()
	for i in range(0,tutrandom):
		
		randomurl = random.randint(1,101)
		
		if randomurl not in lst:
			tmpurl = randomurl
			lst.append(randomurl)
			
			rating = random.randint(1,5)
			
			tmpusr = cnt

			with open("/home/gpawar916/TESTDATA/tutrating.csv","a+") as csvfile:
				writer = csv.DictWriter(csvfile,fieldnames=fieldnames)

				#writer.writeheader()
                                writer.writerow({'userID': tmpusr,'tutID': tmpurl,'rating':rating})

		
	del lst[:]
