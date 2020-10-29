# PageRanking of Airports 
##  Used Google's Page Ranking algorithm to evaluate relative importance of nodes in a connected environment
- Deployed using Scala with Spark framework , implementing an executable jar file in AWS EMR cluster
- Used Airport Traffic data from [Bureau of Transportation](https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236)
- Used Origin and Destination Airport Codes as nodes in the connected environment
- Used the no. of flights the in-links and out-links
- Application takes in input file path, no. of iterations of Page Rank computation, and output path as arguments
- Application writes out the ranks of individual nodes (airports) as a file to the output path

