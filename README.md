# INF727_Shavadoop
Educational project of Simplified Data Processing on Large Clusters, in java.

Il s’agit d’implémenter un « Word Count » reposant sur une architecture répartie scalable, tel MapReduce. Ce « Word Count » classique consiste à compter le nombre d’occurrences de chaque mot contenu dans un document. L’objectif est de réaliser cette opération de façon optimisée (autant que cela a été possible) sur un cluster de machines de TP de l’école.

Notre cluster de machines sera utilisé selon le principe des architectures distribuées, avec une machine principale qui sera notre « Master » et des machines « slaves » qui seront nos « Workers ». Le but est d’optimiser au mieux notre algorithme de « Word Count » en parallélisant et distribuant les différentes tâches sur nos Master et Workers. Ceci afin d’obtenir les meilleures performances. 

Source : http://static.googleusercontent.com/media/research.google.com/fr//archive/mapreduce-osdi04.pdf
