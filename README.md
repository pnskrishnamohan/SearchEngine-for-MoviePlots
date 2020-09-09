1. Search engine is built for movie plot summaries. The dataset used is from the Carnegie Movie Summary Corpus site.
2. Link to dataset: http://www.cs.cmu.edu/~ark/personas/data/MovieSummaries.tar.gz
3. When user enters a single term: Top 10 documents with highest tf-idf values are outputted.
4. When user enters a query consisting of multiple terms: Top 10 documents having highest cosine similarity values are outputted.
5. The query's are listed in 'query.txt' file and the stopwords are read from file 'stopwords.txt'
6. The file movie.metadata.tsv is used to lookup for the movie names.
7. The whole project is implemented in Databricks cluster.
